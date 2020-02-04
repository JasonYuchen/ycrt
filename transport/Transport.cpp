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
  function<string(uint64_t, uint64_t)> &&snapshotDirFunc,
  uint64_t ioContexts)
{
  TransportUPtr t(new Transport(
    nhConfig, resolver, handlers, std::move(snapshotDirFunc), ioContexts));
  t->Start();
  return t;
}

Transport::Transport(
  const NodeHostConfig &nhConfig,
  Nodes &resolver,
  RaftMessageHandler &handlers,
  function<string(uint64_t, uint64_t)> &&snapshotDirFunc,
  uint64_t ioContexts)
  : log(Log.GetLogger("transport")),
    io_(1),
    worker_(io_),
    main_([this](){ io_.run(); }),
    stopped_(false),
    ioctxIdx_(0),
    ioctxs_(),
    acceptor_(io_, getEndpoint(string_view(nhConfig.ListenAddress))),
//    streamConnections_(Soft::ins().StreamConnections),
//    sendQueueLength_(Soft::ins().SendQueueLength),
//    getConnectedTimeoutS_(Soft::ins().GetConnectedTimeoutS),
//    idleTimeoutS_(60), // TODO: add idleTimeoutS to soft?, add idleTimeoutS_ to Asio to handle timeout events
    deploymentID_(nhConfig.DeploymentID),
    mutex_(),
    sendChannels_(),
    sourceAddress_(nhConfig.RaftAddress),
    resolver_(resolver),
    handlers_(handlers)
{
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
      ch = make_shared<SendChannel>(
        this, nextIOContext(), sourceAddress_, node, sendQueueLength_);
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
      "{0} do not have the address for {1}, dropping a message",
      sourceAddress_, FmtClusterNode(m->cluster_id(), m->to()));
    return false;
  }
}

void Transport::Start()
{
  auto conn = make_shared<RecvChannel>(this, nextIOContext());
  acceptor_.async_accept(
    conn->socket(),
    [conn, this](const error_code &ec) mutable {
      if (!ec) {
        log->info("new connection received from {0}", conn->socket().remote_endpoint().address().to_string());
        conn->SetRequestHandler(bind(&Transport::handleRequest, this, std::placeholders::_1));
        conn->SetChunkHandler(bind(&Transport::handleSnapshotChunk, this, std::placeholders::_1));
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

io_context &Transport::nextIOContext()
{
  return ioctxs_[ioctxIdx_.fetch_add(1) % ioctxs_.size()]->io;
}

void Transport::handleRequest(pbMessageBatchUPtr m)
{
  if (m->deployment_id() != deploymentID_) {
    log->warn(
      "deployment id does not match,"
      " received {0:d}, actual {1:d}, message dropped",
      m->deployment_id(), deploymentID_);
    return;
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
}

void Transport::handleSnapshotChunk(pbSnapshotChunkSPtr m)
{
  // FIXME
}

void Transport::handleSnapshotConfirm(
  uint64_t clusterID,
  uint64_t nodeID,
  uint64_t from)
{
  handlers_.handleSnapshot(clusterID, nodeID, from);
}

void Transport::handleUnreachable(const std::string &address)
{
  vector<NodeInfo> remotes = resolver_.ReverseResolve(address);
  log->info("node {0} becomes unreachable, affecting {1} raft nodes",
    address, remotes.size());
  for (auto &node : remotes) {
    handlers_.handleUnreachable(node.ClusterID, node.NodeID);
  }
}

} // namespace transport

} // namespace ycrt