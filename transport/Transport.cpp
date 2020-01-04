//
// Created by jason on 2019/12/21.
//

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

TransportSPtr Transport::New(
  NodeHostConfigSPtr nhConfig,
  NodesSPtr resolver,
  RaftMessageHandlerSPtr handlers,
  function<string(uint64_t, uint64_t)> &&snapshotDirFunc,
  uint64_t ioContexts)
{
  TransportSPtr transport(new Transport(std::move(nhConfig)));
  transport->resolver_ = std::move(resolver);
  transport->handlers_ = std::move(handlers);
  for (size_t i = 0; i < ioContexts; ++i) {
    transport->ioctxs_.emplace_back(new ioctx());
  }
  return transport;
}

Transport::Transport(NodeHostConfigSPtr nhConfig)
  : log(Log.GetLogger("transport")),
    nhConfig_(std::move(nhConfig)),
    io_(1),
    worker_(io_),
    main_([this](){ io_.run(); }),
    stopped_(false),
    ioctxIdx_(0),
    ioctxs_(),
    acceptor_(io_, getEndpoint(string_view(nhConfig_->ListenAddress))),
    streamConnections_(Soft::ins().StreamConnections),
    sendQueueLength_(Soft::ins().SendQueueLength),
    getConnectedTimeoutS_(Soft::ins().GetConnectedTimeoutS),
    idleTimeoutS_(60), // TODO: add idleTimeoutS to soft?
    deploymentID_(nhConfig_->DeploymentID),
    mutex_(),
    sendChannels_(),
    sourceAddress_(nhConfig_->RaftAddress),
    resolver_(),
    handlers_()
{
  log->info("start listening on {0}", nhConfig_->ListenAddress);
}

bool Transport::AsyncSendMessage(pbMessageSPtr m)
{
  NodesRecordSPtr node = resolver_->Resolve(m->cluster_id(), m->to());
  if (node == nullptr) {
    log->warn(
      "{0} do not have the address for {1:d}:{2:d}, dropping a message",
      sourceAddress_, m->cluster_id(), m->to());
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
    }
  }
  ch->AsyncSendMessage(std::move(m));
  return true;
}

void Transport::Start()
{
  auto conn = make_shared<RecvChannel>(this, nextIOContext());
  acceptor_.async_accept(
    conn->socket(),
    [conn, this](const error_code &ec) mutable {
      if (!ec) {
        log->info("new connection received from {0}", conn->socket().remote_endpoint().address().to_string());
        conn->SetRequestHandlerPtr(
          [this](pbMessageBatchUPtr m)
          {
            if (m->deployment_id() != deploymentID_) {
              log->warn("deployment id does not match,"
                        " received {0:d}, actual {1:d}, message dropped",
                        m->deployment_id(), deploymentID_);
              return;
            }
            // FIXME: Check RPC Bin Ver
            const string &addr = m->source_address();
            if (!addr.empty()) {
              for (auto &req : m->requests()) {
                if (req.from() != 0) {
                  log->info("new remote address learnt: {0:d}:{1:d} in {2}",
                    req.cluster_id(), req.from(), addr);
                  resolver_->AddRemoteAddress(
                    req.cluster_id(), req.from(), addr);
                }
              }
            }
            handlers_->handleMessageBatch(std::move(m));
            // TODO: metrics
          });
        conn->SetChunkHandlerPtr(
          [this](pbSnapshotChunkSPtr m)
          {
            // TODO
            log->warn("snapshot chunk not supported currently");
          });
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

} // namespace transport

} // namespace ycrt