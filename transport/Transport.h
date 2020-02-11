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
#include "NodeResolver.h"
#include "Channel.h"
#include "SnapshotChunkManager.h"

namespace ycrt
{

namespace transport
{

class Transport {
 public:
  DISALLOW_COPY_AND_ASSIGN(Transport);
  static std::unique_ptr<Transport> New(
    const NodeHostConfig  &nhConfig,
    NodeResolver &resolver,
    RaftMessageHandler &handlers,
    server::SnapshotLocator &&locator,
    uint64_t ioContexts);
  uint64_t GetDeploymentID() { return deploymentID_; }

  bool AsyncSendMessage(pbMessageUPtr m);
  bool AsyncSendSnapshot(pbMessageUPtr m);
  // TODO:
  //  SNAPSHOT STREAMING
  //  std::shared_ptr<Sink> GetStreamConnection(uint64_t clusterID, uint64_t nodeID);
  void Stop();
  void RemoveSendChannel(const std::string &key);

  // send snapshot notification (succeed or failed), called by SnapshotLane
  void SendSnapshotNotification(NodeInfo node, bool reject);
  // receive a normal message, called by RecvChannel
  bool HandleRequest(pbMessageBatchUPtr m);
  // receive a snapshot chunk, called by RecvChannel
  bool HandleSnapshotChunk(pbSnapshotChunkSPtr m);
  // receive the last piece of snapshot and notify the corresponding cluster, called by SnapshotChunkManager
  bool HandleSnapshotConfirm(NodeInfo node, uint64_t from);
  // remote node is unreachable, notify the corresponding cluster, called by SendChannel
  bool HandleUnreachable(const std::string &address);

  ~Transport();
 private:
  explicit Transport(
    const NodeHostConfig  &nhConfig,
    NodeResolver &resolver,
    RaftMessageHandler &handlers,
    std::function<std::string(uint64_t, uint64_t)> &&snapshotDirFunc,
    uint64_t ioContexts);
  void start();
  boost::asio::io_context &nextIOContext();

  const uint64_t deploymentID_;
  const uint64_t streamConnections_;
  const uint64_t sendQueueLength_;
  const uint64_t getConnectedTimeoutS_;
  const uint64_t idleTimeoutS_;
  const uint64_t maxSnapshotLanes_;

  slogger log;
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
  std::vector<std::unique_ptr<ioctx>> ioctxs_;
  boost::asio::ip::tcp::acceptor acceptor_;

  std::mutex mutex_;
  std::unordered_map<std::string, SendChannelSPtr> sendChannels_; // GUARDED BY mutex_;
  // TODO: enable CircuitBreaker
  //  std::unordered_map<std::string, CircuitBreaker> breakers_; // GUARDED BY mutex_;
  std::atomic_uint64_t lanes_;
  // TODO: enable metrics
  //  TransportMetrics metrics_;
  std::string sourceAddress_;
  NodeResolver &resolver_; // owned by Node
  SnapshotChunkManagerUPtr chunkManager_;
  RaftMessageHandler &handlers_; // owned by NodeHost
};
using TransportUPtr = std::unique_ptr<Transport>;

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_TRANSPORT_H_
