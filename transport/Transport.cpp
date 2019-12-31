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

unique_ptr<Transport> Transport::New(
  NodeHostConfigSPtr nhConfig,
  NodeAddressResolverSPtr resolver,
  RaftMessageHandlerSPtr handlers,
  function<std::string(uint64_t, uint64_t)> snapshotDirFunc,
  uint64_t ioContexts)
{
  auto transport = make_unique<Transport>();
  transport->nhConfig_ = std::move(nhConfig);
  transport->resolver_ = std::move(resolver);
  transport->handlers_ = std::move(handlers);
  transport->deploymentID_ = nhConfig_->DeploymentID;
  transport->ioctxs_.resize(ioContexts);
  return transport;
}

Transport::Transport()
  : log(Log.get("transport")),
    io_(),
    acceptor_(io_),
    streamConnections_(Soft::ins().StreamConnections),
    sendQueueLength_(Soft::ins().SendQueueLength),
    getConnectedTimeoutS_(Soft::ins().GetConnectedTimeoutS),
    idleTimeoutS_(60), // TODO: add idleTimeoutS to soft?
    deploymentID_(0)
{
}

bool Transport::asyncSendMessage(MessageUPtr m)
{
  shared_ptr<NodeInfo> node =
    resolver_->resolve(m->cluster_id(), m->to());
  if (node == nullptr) {
    log->warn(
      "{0} do not have the address for {1}:{2}, dropping a message",
      sourceAddress_, m->cluster_id(), m->to());
  }
  auto ch = sendChannels_.find(node->key);
  string test;
  if (ch == sendChannels_.end()) {
    sendChannels_[node->key] = make_unique<SendChannel>(io_, node, sendQueueLength_);
    //sendChannels_.insert({test, new Channel(io_, node, sendQueueLength_)});
    ch = sendChannels_.find(node->key);
  }
  ch->second->asyncSendMessage(std::move(m));
}

void Transport::start()
{
  auto pos = nhConfig_->ListenAddress.find(':');
  ip::tcp::endpoint endpoint(
    ip::make_address(nhConfig_->ListenAddress),
    stoi(nhConfig_->ListenAddress.substr(pos)));
  auto conn = make_shared<RecvChannel>(nextIOContext());
  acceptor_.async_accept(
    conn->socket(),
    [conn = std::move(conn), this](const error_code &error) mutable {
      if (error) {
        log->warn("accept error: {0}", error.message());
      } else {
        conn->setRequestHandlerPtr(
          [this](MessageBatchUPtr m)
          {
            if (m->deployment_id() != deploymentID_) {
              log->warn("deployment id does not match,"
                        " received {0:d}, actual {1:d}, message dropped",
                        m->deployment_id(), deploymentID_);
            }
            // FIXME: Check RPC Bin Ver
            const string &addr = m->source_address();
            if (!addr.empty()) {
              for (auto &req : m->requests()) {
                if (req.from() != 0) {
                  resolver_->addRemoteAddress(req.cluster_id(), req.from(), addr);
                }
              }
            }
            handlers_->handleMessageBatch(std::move(m));
            // TODO: metrics
          });
        conn->setChunkHandlerPtr(
          [this](SnapshotChunkUPtr m)
          {
            // TODO
          });
        conn->start();
      }
      start();
    });
}
boost::asio::io_context &Transport::nextIOContext()
{
  static uint64_t idx = 0;
  return ioctxs_[idx++ % ioctxs_.size()].io;
}

} // namespace transport

} // namespace ycrt