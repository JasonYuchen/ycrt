//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_TRANSPORT_TRANSPORT_H_
#define YCRT_TRANSPORT_TRANSPORT_H_

#include <memory>
#include <queue>
#include <utility>
#include <vector>
#include <boost/asio.hpp>
#include "ycrt/Config.h"
#include "pb/RaftMessage.h"
#include "utils/Utils.h"
#include "settings/Hard.h"

namespace ycrt
{

namespace transport
{



class Transport {
 public:
  std::unique_ptr<Transport> New(
    NodeHostConfigSPtr nhConfig,
    NodeAddressResolverSPtr resolver,
    RaftMessageHandlerSPtr handlers,
    std::function<std::string(uint64_t, uint64_t)> snapshotDirFunc,
    uint64_t ioContexts);
  Transport();
  std::string name();
  //void setUnmanagedDeploymentID();
  void setDeploymentID(uint64_t id);

  bool asyncSendMessage(MessageUPtr m);
  bool asyncSendSnapshot(MessageUPtr m);
  //std::shared_ptr<Sink> getStreamConnection(uint64_t clusterID, uint64_t nodeID);
  void start();
  void stop() { io_.stop(); }
 private:
  slogger log;
  boost::asio::io_context io_;
  boost::asio::ip::tcp::acceptor acceptor_;
  uint64_t streamConnections_;
  uint64_t sendQueueLength_;
  uint64_t getConnectedTimeoutS_;
  uint64_t idleTimeoutS_;

  uint64_t deploymentID;
  std::mutex mutex_;
  std::unordered_map<std::string, SendChannelUPtr> sendChannels_; // GUARDED BY mutex_;
  BlockingConcurrentQueueUPtr<MessageBatchUPtr> outputQueue_;
  // std::unordered_map<std::string, CircuitBreaker> breakers_;
  uint32_t lanes_;
  // TransportMetrics metrics_;
  // server::Context serverCtx_;
  NodeHostConfigSPtr nhConfig_;
  std::string sourceAddress_;
  NodeAddressResolverSPtr resolver_;
  RaftMessageHandlerSPtr handlers_;
};

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_TRANSPORT_H_
