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

static vector<pbSnapshotChunkSPtr> getWitnessChunks(const pbMessage &m)
{
  StatusWith<string> data{ErrorCode::OK}; // TODO: rsm.GetWitnessSnapshot()
  data.IsOKOrThrow();
  vector<pbSnapshotChunkSPtr> chunks;
  auto chunk = make_shared<pbSnapshotChunk>();
  chunk->set_cluster_id(m.cluster_id());
  chunk->set_node_id(m.to());
  chunk->set_from(m.from());
  chunk->set_file_chunk_id(0);
  chunk->set_file_chunk_count(1);
  chunk->set_chunk_id(0);
  chunk->set_chunk_size(data.GetMutableOrThrow().size());
  chunk->set_index(m.snapshot().index());
  chunk->set_term(m.snapshot().term());
  chunk->set_on_disk_index(0);
  chunk->set_allocated_membership(new pbMembership(m.snapshot().membership()));
  chunk->set_filepath("witness.snapshot");
  chunk->set_file_size(data.GetMutableOrThrow().size());
  chunk->set_witness(true);
  chunk->set_data(std::move(data.GetMutableOrThrow()));
  chunks.emplace_back(std::move(chunk));
  return chunks;
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
    return getWitnessChunks(m);
  } else {
    return getChunks(m);
  }
}

TransportUPtr Transport::New(
  const NodeHostConfig &nhConfig,
  NodeResolver &resolver,
  RaftMessageHandler &handlers,
  server::SnapshotLocator &&locator,
  uint64_t ioContexts)
{
  TransportUPtr t(new Transport(
    nhConfig, resolver, handlers, std::move(locator), ioContexts));
  t->log->info("Transport: start listening on {}", nhConfig.ListenAddress);
  t->start();
  return t;
}

Transport::Transport(
  const NodeHostConfig &nhConfig,
  NodeResolver &resolver,
  RaftMessageHandler &handlers,
  server::SnapshotLocator &&locator,
  uint64_t ioContexts)
  : deploymentID_(nhConfig.DeploymentID),
    streamConnections_(Soft::ins().StreamConnections),
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
    mutex_(),
    sendChannels_(),
    lanes_(0),
    sourceAddress_(nhConfig.RaftAddress),
    resolver_(resolver),
    chunkManager_(SnapshotChunkManager::New(*this, io_, std::move(locator))),
    handlers_(handlers)
{
  for (size_t i = 0; i < ioContexts; ++i) {
    ioctxs_.emplace_back(new ioctx());
  }
  chunkManager_->RunTicker();
}

bool Transport::AsyncSendMessage(pbMessageUPtr m)
{
  if (m->type() == raftpb::InstallSnapshot) {
    throw Error(ErrorCode::UnexpectedRaftMessage, log,
      "Transport::AsyncSendMessage: snapshot must be sent via its own channel");
  }
  NodeInfo node{m->cluster_id(), m->to()};
  NodesRecordSPtr nodeRecord = resolver_.Resolve(node);
  if (nodeRecord == nullptr) {
    log->warn("Transport::AsyncSendMessage: "
              "{} do not have the address for {}, dropping a message",
              sourceAddress_, node);
    return false;
  }
  SendChannelSPtr ch;
  {
    lock_guard<mutex> guard(mutex_);
    auto it = sendChannels_.find(nodeRecord->Key);
    if (it == sendChannels_.end()) {
      // FIXME: add CircuitBreaker
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
      "Transport::AsyncSendSnapshot: snapshot channel only sends snapshots");
  }
  NodeInfo node{m->cluster_id(), m->to()};
  NodesRecordSPtr nodeRecord = resolver_.Resolve(node);
  if (nodeRecord == nullptr) {
    log->warn("Transport::AsyncSendSnapshot: "
              "{} do not have the address for {}, dropping a snapshot",
              sourceAddress_, node);
    SendSnapshotNotification(node, true);
    return false;
  }
  vector<pbSnapshotChunkSPtr> chunks = splitSnapshotMessage(*m);
  // TODO: add CircuitBreaker
  if (lanes_ > maxSnapshotLanes_) {
    log->warn("Transport::AsyncSendSnapshot: "
              "snapshot lane count exceeds maxSnapshotLanes={}, abort",
              maxSnapshotLanes_);
    SendSnapshotNotification(node, true);
    return false;
  }
  auto lane = make_shared<SnapshotLane>(
    *this, lanes_, nextIOContext(), sourceAddress_, nodeRecord, node);
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
    log->info("Transport::Stop: successful");
  }
}

void Transport::RemoveSendChannel(const string &key)
{
  lock_guard<mutex> guard(mutex_);
  log->debug("Transport::RemoveSendChannel: channel {} removed", key);
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
  log->debug("Transport::SendSnapshotNotification: {} reject={}", node, reject);
  handlers_.handleSnapshotStatus(node, reject);
}

bool Transport::HandleRequest(pbMessageBatchUPtr m)
{
  if (m->deployment_id() != deploymentID_) {
    log->warn("Transport::HandleRequest: deployment id does not match, "
              "received {}, expected {}, message dropped",
              m->deployment_id(), deploymentID_);
    return false;
  }
  const string &addr = m->source_address();
  if (!addr.empty()) {
    for (auto &req : m->requests()) {
      if (req.from() != 0) {
        log->debug("Transport::HandleRequest: new remote address found: {} {}",
          NodeInfo{req.cluster_id(), req.from()}, addr);
        resolver_.AddRemoteAddress(
          NodeInfo{req.cluster_id(), req.from()}, addr);
      }
    }
  }
  log->debug("Transport::HandleRequest: received from {}", m->source_address());
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
  log->debug("Transport::HandleSnapshotConfirm: {} from={}", node, from);
  handlers_.handleSnapshot(node, from);
  return true;
}

bool Transport::HandleUnreachable(const std::string &address)
{
  vector<NodeInfo> remotes = resolver_.ReverseResolve(address);
  log->info("Transport::HandleUnreachable: "
            "{} becomes unreachable, affecting {} raft nodes",
            address, remotes.size());
  for (auto &node : remotes) {
    log->debug("Transport::HandleUnreachable: {} becomes unreachable", node);
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
        log->info("Transport::start: new connection received from {}",
          conn->socket().remote_endpoint().address().to_string());
        conn->Start();
      } else if (ec.value() == error::operation_aborted) {
        log->debug("Transport::start: operation aborted");
        return;
      } else {
        log->warn("Transport::start: async_accept error: {}", ec.message());
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