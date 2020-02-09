//
// Created by jason on 2019/12/21.
//

#include <functional>

#include "Channel.h"
#include "pb/raft.pb.h"
#include "Transport.h"
#include "settings/Soft.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace transport
{

using namespace settings;
using namespace std;
using namespace boost::asio;
using boost::system::error_code;
using boost::system::system_error;

// TODO: move this method to pbMessage
static uint64_t splitBySnapshotFile(
  vector<pbSnapshotChunkSPtr> &chunks,
  uint64_t chunkID,
  const pbMessage &m,
  const string &filePath,
  uint64_t fileSize,
  pbSnapshotFile *allocatedFile)
{
  if (fileSize == 0) {
    throw Error(ErrorCode::UnexpectedRaftMessage,
      "empty file included in snapshot");
  }
  uint64_t chunkCount = (fileSize - 1) / settings::SnapshotChunkSize + 1;
  Log.GetLogger("transport")->info(
    "splitBySnapshotFile: chunk count={}, file size={}",
    chunkCount, fileSize);
  for (uint64_t i = 0; i < chunkCount; ++i) {
    uint64_t chunkSize = 0;
    if (i == chunkCount - 1) {
      chunkSize = fileSize - (chunkCount - 1) * settings::SnapshotChunkSize;
    } else {
      chunkSize = settings::SnapshotChunkSize;
    }
    // leave chunk->data unset (will be filled in sending procedure)
    auto chunk = make_shared<pbSnapshotChunk>();
    chunk->set_cluster_id(m.cluster_id());
    chunk->set_node_id(m.to());
    chunk->set_from(m.from());
    chunk->set_file_chunk_id(i);
    chunk->set_file_chunk_count(chunkCount);
    chunk->set_chunk_id(chunkID + i);
    chunk->set_chunk_size(chunkSize);
    chunk->set_index(m.snapshot().index());
    chunk->set_term(m.snapshot().term());
    chunk->set_on_disk_index(m.snapshot().on_disk_index());
    chunk->set_allocated_membership(new pbMembership(m.snapshot().membership()));
    chunk->set_filepath(filePath);
    chunk->set_file_size(fileSize);
    chunk->set_witness(m.snapshot().witness());
    if (allocatedFile != nullptr) {
      chunk->set_allocated_file_info(allocatedFile);
    }
    chunks.push_back(std::move(chunk));
  }
  return chunkCount;
}

static vector<pbSnapshotChunkSPtr> getChunks(const pbMessage &m)
{
  uint64_t startChunkID = 0;
  vector<pbSnapshotChunkSPtr> chunks;
  uint64_t count = splitBySnapshotFile(
    chunks,
    startChunkID,
    m,
    m.snapshot().filepath(),
    m.snapshot().file_size(),
    nullptr);
  startChunkID += count;
  for (auto &file : m.snapshot().files()) {
    count = splitBySnapshotFile(
      chunks,
      startChunkID,
      m,
      file.file_path(),
      file.file_size(),
      new pbSnapshotFile(file));
    startChunkID += count;
  }
  for (auto &chunk : chunks) {
    chunk->set_chunk_count(chunks.size());
  }
  return chunks;
}
static vector<pbSnapshotChunkSPtr> splitSnapshotMessage(const pbMessage &m)
{
  if (m.snapshot().witness()) {
    // TODO: return getWitnessChunks(m);
    throw Error(ErrorCode::Other, "witness snapshot not supported");
  } else {
    return getChunks(m);
  }
}

TransportUPtr Transport::New(
  const NodeHostConfig &nhConfig,
  Nodes &resolver,
  RaftMessageHandler &handlers,
  server::SnapshotLocator &&locator,
  uint64_t ioContexts)
{
  TransportUPtr t(new Transport(
    nhConfig, resolver, handlers, std::move(locator), ioContexts));
  t->log->info("start listening on {}", nhConfig.ListenAddress);
  t->start();
  return t;
}

Transport::Transport(
  const NodeHostConfig &nhConfig,
  Nodes &resolver,
  RaftMessageHandler &handlers,
  server::SnapshotLocator &&locator,
  uint64_t ioContexts)
  : streamConnections_(Soft::ins().StreamConnections),
    sendQueueLength_(Soft::ins().SendQueueLength),
    getConnectedTimeoutS_(Soft::ins().GetConnectedTimeoutS),
    idleTimeoutS_(60), // TODO: add idleTimeoutS to soft?, add idleTimeoutS_ to Asio to handle timeout events
    maxSnapshotLanes_(Soft::ins().MaxSnapshotConnections),
    log(Log.GetLogger("transport")),
    io_(1),
    worker_(io_),
    main_([this](){ io_.run(); }),
    stopped_(false),
    ioctxIdx_(0),
    ioctxs_(),
    acceptor_(io_, getEndpoint(string_view(nhConfig.ListenAddress))),
    deploymentID_(nhConfig.DeploymentID),
    mutex_(),
    sendChannels_(),
    breakers_(),
    lanes_(0),
    sourceAddress_(nhConfig.RaftAddress),
    resolver_(resolver),
    chunkManager_(SnapshotChunkManager::New(*this, io_, std::move(locator))),
    handlers_(handlers)
{
  // TODO: run timer on SnapshotChunkManager
  for (size_t i = 0; i < ioContexts; ++i) {
    ioctxs_.emplace_back(new ioctx());
  }
  chunkManager_->RunTicker();
}

bool Transport::AsyncSendMessage(pbMessageUPtr m)
{
  if (m->type() == raftpb::InstallSnapshot) {
    throw Error(ErrorCode::UnexpectedRaftMessage, log,
      "Snapshot must be sent via its own channel");
  }
  NodeInfo node{m->cluster_id(), m->to()};
  NodesRecordSPtr nodeRecord = resolver_.Resolve(node);
  if (nodeRecord == nullptr) {
    log->warn(
      "{} do not have the address for {}, dropping a message",
      sourceAddress_, node);
    return false;
  }
  SendChannelSPtr ch;
  {
    lock_guard<mutex> guard(mutex_);
    auto it = sendChannels_.find(nodeRecord->Key);
    if (it == sendChannels_.end()) {
      // TODO: add CircuitBreaker
      ch = make_shared<SendChannel>(
        *this, nextIOContext(), sourceAddress_, nodeRecord, sendQueueLength_);
      sendChannels_[nodeRecord->Key] = ch;
      ch->Start();
    } else {
      ch = it->second;
    }
  }
  ch->AsyncSendMessage(std::move(m));
  return true;
}

bool Transport::AsyncSendSnapshot(pbMessageUPtr m)
{
  if (m->type() != raftpb::InstallSnapshot) {
    throw Error(ErrorCode::UnexpectedRaftMessage, log,
      "Snapshot channel only sends snapshots");
  }
  NodeInfo node{m->cluster_id(), m->to()};
  NodesRecordSPtr nodeRecord = resolver_.Resolve(node);
  if (nodeRecord == nullptr) {
    log->warn(
      "{} do not have the address for {}, dropping a snapshot",
      sourceAddress_, node);
    handlers_.handleSnapshotStatus(node, true);
    return false;
  }
  vector<pbSnapshotChunkSPtr> chunks = splitSnapshotMessage(*m);
  // TODO: add CircuitBreaker
  if (lanes_ > maxSnapshotLanes_) {
    log->warn("snapshot lane count exceeds maxSnapshotLanes, abort");
    handlers_.handleSnapshotStatus(node, true);
    return false;
  }
  auto lane = make_shared<SnapshotLane>(
    *this,
    lanes_,
    nextIOContext(),
    sourceAddress_,
    nodeRecord,
    node);
  lane->Start(std::move(chunks));
  return true;
}

void Transport::Stop()
{
  if (!stopped_.exchange(true)) {
    {
      lock_guard<mutex> guard(mutex_);
      sendChannels_.clear();
    }
    ioctxs_.clear();
    io_.stop();
    main_.join();
  }
}

void Transport::RemoveSendChannel(const string &key)
{
  lock_guard<mutex> guard(mutex_);
  sendChannels_.erase(key);
}

Transport::~Transport()
{
  if (!stopped_) {
    Stop();
  }
}

void Transport::SendSnapshotNotification(NodeInfo node, bool reject)
{
  handlers_.handleSnapshotStatus(node, reject);
}

bool Transport::HandleRequest(pbMessageBatchUPtr m)
{
  if (m->deployment_id() != deploymentID_) {
    log->warn(
      "deployment id does not match,"
      " received {0:d}, actual {1:d}, message dropped",
      m->deployment_id(), deploymentID_);
    return false;
  }
  // FIXME: Check RPC Bin Ver
  const string &addr = m->source_address();
  if (!addr.empty()) {
    for (auto &req : m->requests()) {
      if (req.from() != 0) {
        log->info(
          "new remote address learnt: {} in {}",
          NodeInfo{req.cluster_id(), req.from()}, addr);
        resolver_.AddRemoteAddress(
          NodeInfo{req.cluster_id(), req.from()}, addr);
      }
    }
  }
  handlers_.handleMessageBatch(std::move(m));
  // TODO: metrics
  return true;
}

bool Transport::HandleSnapshotChunk(pbSnapshotChunkSPtr m)
{
  return chunkManager_->AddChunk(std::move(m));
}

bool Transport::HandleSnapshotConfirm(NodeInfo node, uint64_t from)
{
  handlers_.handleSnapshot(node, from);
  return true;
}

bool Transport::HandleUnreachable(const std::string &address)
{
  vector<NodeInfo> remotes = resolver_.ReverseResolve(address);
  log->info("node {} becomes unreachable, affecting {} raft nodes",
    address, remotes.size());
  for (auto &node : remotes) {
    handlers_.handleUnreachable(node);
  }
  return true;
}

void Transport::start()
{
  auto conn = make_shared<RecvChannel>(*this, nextIOContext());
  acceptor_.async_accept(
    conn->socket(),
    [conn, this](const error_code &ec) mutable {
      if (!ec) {
        log->info("new connection received from {}", conn->socket().remote_endpoint().address().to_string());
        conn->Start();
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->warn("async_accept error {}", ec.message());
      }
      start();
    });
}

io_context &Transport::nextIOContext()
{
  return ioctxs_[ioctxIdx_.fetch_add(1) % ioctxs_.size()]->io;
}

} // namespace transport

} // namespace ycrt