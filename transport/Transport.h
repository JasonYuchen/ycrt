//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_TRANSPORT_TRANSPORT_H_
#define YCRT_TRANSPORT_TRANSPORT_H_

#include <memory>
#include <queue>
#include <utility>
#include <vector>
#include <thread>
#include <boost/asio.hpp>
#include "ycrt/Config.h"
#include "pb/RaftMessage.h"
#include "utils/Utils.h"
#include "settings/Hard.h"
#include "Nodes.h"
#include "Channel.h"

namespace ycrt
{

namespace transport
{

// TODO: format log output
// TODO: enable CircuitBreaker
// TODO: asyncSendSnapshot
// TODO: snapshot streaming

class Transport {
 public:
  static std::shared_ptr<Transport> New(
    NodeHostConfigSPtr nhConfig,
    NodesSPtr resolver,
    RaftMessageHandlerSPtr handlers,
    std::function<std::string(uint64_t, uint64_t)> &&snapshotDirFunc,
    uint64_t ioContexts);
  //std::string name();
  //void setUnmanagedDeploymentID();
  void SetDeploymentID(uint64_t id) { deploymentID_ = id; }
  uint64_t GetDeploymentID() { return deploymentID_; }

  bool AsyncSendMessage(MessageUPtr m);
  //bool AsyncSendSnapshot(MessageUPtr m);
  //std::shared_ptr<Sink> GetStreamConnection(uint64_t clusterID, uint64_t nodeID);
  void Start();
  void Stop();
  void RemoveSendChannel(const std::string &key);
  ~Transport();
 private:
  Transport(NodeHostConfigSPtr);
  boost::asio::io_context &nextIOContext();
  slogger log;
  NodeHostConfigSPtr nhConfig_;
  boost::asio::io_context io_;
  boost::asio::io_context::work worker_;
  std::thread main_;
  std::atomic_bool stopped_;
  struct ioctx {
    ioctx() : io(1), worker(io), executor([this](){io.run();}) {}
    ~ioctx() { io.stop(); executor.join(); }
    boost::asio::io_context io;
    boost::asio::io_context::work worker;
    std::thread executor;
  };
  std::atomic_uint64_t ioctxIdx_;
  std::vector<std::shared_ptr<ioctx>> ioctxs_;
  boost::asio::ip::tcp::acceptor acceptor_;
  uint64_t streamConnections_;
  uint64_t sendQueueLength_;
  uint64_t getConnectedTimeoutS_;
  uint64_t idleTimeoutS_;
  uint64_t deploymentID_;

  std::mutex mutex_;
  std::unordered_map<std::string, SendChannelSPtr> sendChannels_; // GUARDED BY mutex_;
  // BlockingConcurrentQueueUPtr<MessageBatchUPtr> outputQueue_;
  // std::unordered_map<std::string, CircuitBreaker> breakers_;
  // uint32_t lanes_;
  // TransportMetrics metrics_;
  // server::Context serverCtx_;
  std::string sourceAddress_;
  NodesSPtr resolver_;
  RaftMessageHandlerSPtr handlers_;
};
using TransportSPtr = std::shared_ptr<Transport>;

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_TRANSPORT_H_
