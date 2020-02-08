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

TransportUPtr Transport::New(
  const NodeHostConfig &nhConfig,
  Nodes &resolver,
  RaftMessageHandler &handlers,
  server::SnapshotLocator &&locator,
  uint64_t ioContexts)
{
  TransportUPtr t(new Transport(
    nhConfig, resolver, handlers, std::move(locator), ioContexts));
  t->Start();
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
    sourceAddress_(nhConfig.RaftAddress),
    resolver_(resolver),
    chunkManager_(SnapshotChunkManager::New(*this, std::move(locator))),
    handlers_(handlers)
{
  // TODO: run timer on SnapshotChunkManager
  for (size_t i = 0; i < ioContexts; ++i) {
    ioctxs_.emplace_back(new ioctx());
  }
  log->info("start listening on {0}", nhConfig.ListenAddress);
}

bool Transport::AsyncSendMessage(pbMessageUPtr m)
{
  if (m->type() == raftpb::InstallSnapshot) {
    throw Error(ErrorCode::UnexpectedRaftMessage, log,
      "Snapshot must be sent via its own channel");
  }
  NodesRecordSPtr node = resolver_.Resolve(m->cluster_id(), m->to());
  if (node == nullptr) {
    log->warn(
      "{0} do not have the address for {1}, dropping a message",
      sourceAddress_, FmtClusterNode(m->cluster_id(), m->to()));
    return false;
  }
  SendChannelSPtr ch;
  {
    lock_guard<mutex> guard(mutex_);
    auto it = sendChannels_.find(node->Key);
    if (it == sendChannels_.end()) {
      // TODO: add CircuitBreaker
      ch = make_shared<SendChannel>(
        *this, nextIOContext(), sourceAddress_, node, sendQueueLength_);
      sendChannels_[node->Key] = ch;
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
  NodesRecordSPtr node = resolver_.Resolve(m->cluster_id(), m->to());
  if (node == nullptr) {
    log->warn(
      "{0} do not have the address for {1}, dropping a snapshot",
      sourceAddress_, FmtClusterNode(m->cluster_id(), m->to()));
    handlers_.handleSnapshotStatus(m->cluster_id(), m->to(), true);
    return false;
  }
  vector<pbSnapshotChunkSPtr> chunks;// TODO = splitSnapshotMessage(*m);
  // TODO: add CircuitBreaker
  if (lanes_ > maxSnapshotLanes_) {
    log->warn("snapshot lane count exceeds maxSnapshotLanes, abort");
    handlers_.handleSnapshotStatus(m->cluster_id(), m->to(), true);
    return false;
  }
  auto lane = make_shared<SnapshotLane>(
    *this,
    lanes_,
    nextIOContext(),
    sourceAddress_,
    node,
    NodeInfo{m->cluster_id(), m->to()});
  lane->Start(std::move(chunks));
  return true;
}

void Transport::Start()
{
  auto conn = make_shared<RecvChannel>(*this, nextIOContext());
  acceptor_.async_accept(
    conn->socket(),
    [conn, this](const error_code &ec) mutable {
      if (!ec) {
        log->info("new connection received from {0}", conn->socket().remote_endpoint().address().to_string());
        conn->Start();
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->warn("async_accept error {0}", ec.message());
      }
      Start();
    });
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

void Transport::SendSnapshotNotification(
  uint64_t clusterID,
  uint64_t nodeID,
  bool rej)
{
  handlers_.handleSnapshotStatus(clusterID, nodeID, rej);
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
          "new remote address learnt: {0:d}:{1:d} in {2}",
          req.cluster_id(), req.from(), addr);
        resolver_.AddRemoteAddress(
          req.cluster_id(), req.from(), addr);
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

bool Transport::HandleSnapshotConfirm(
  uint64_t clusterID,
  uint64_t nodeID,
  uint64_t from)
{
  handlers_.handleSnapshot(clusterID, nodeID, from);
  return true;
}

bool Transport::HandleUnreachable(const std::string &address)
{
  vector<NodeInfo> remotes = resolver_.ReverseResolve(address);
  log->info("node {0} becomes unreachable, affecting {1} raft nodes",
    address, remotes.size());
  for (auto &node : remotes) {
    handlers_.handleUnreachable(node.ClusterID, node.NodeID);
  }
  return true;
}

io_context &Transport::nextIOContext()
{
  return ioctxs_[ioctxIdx_.fetch_add(1) % ioctxs_.size()]->io;
}

} // namespace transport

} // namespace ycrt