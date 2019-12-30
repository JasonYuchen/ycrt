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

namespace ycrt
{

namespace transport
{

class NodeInfo {
 public:
  std::string key;
  boost::asio::ip::tcp::resolver::results_type endpoints;
};
using NodeInfoSPtr = std::shared_ptr<NodeInfo>;

class NodeAddressResolver {
 public:
  NodeInfoSPtr resolve(uint64_t clusterID, uint64_t nodeID);
 private:
};
using NodeAddressResolverSPtr = std::shared_ptr<NodeAddressResolver>;

class NodeHost;
class RaftMessageHandler {
 public:
  std::pair<uint64_t, uint64_t> handleMessageBatch(MessageBatchUPtr batch);
  void handleUnreachable(uint64_t clusterID, uint64_t nodeID);
  void handleSnapshotStatus(uint64_t clusterID, uint64_t nodeID, bool rejected);
  void handleSnapshot(uint64_t clusterID, uint64_t nodeID, uint64_t from);
 private:
  std::weak_ptr<NodeHost> nh_;
};
using RaftMessageHandlerSPtr = std::shared_ptr<RaftMessageHandler>;

class Channel : public std::enable_shared_from_this<Channel> {
 public:
  explicit Channel(boost::asio::io_context &io, NodeInfoSPtr node, uint64_t queueLength)
    : io_(io),
      socket_(io),
      nodeInfo_(std::move(node)),
      bufferQueue_(std::make_shared<BlockingConcurrentQueue<MessageUPtr>>(queueLength))
  {
    connect();
  }
  explicit Channel(boost::asio::io_context &io, NodeInfoSPtr node, BlockingConcurrentQueueSPtr<MessageUPtr> &queue)
    : io_(io),
      socket_(io),
      nodeInfo_(std::move(node)),
      bufferQueue_(queue)
  {
    connect();
  }
  bool asyncSendMessage(MessageUPtr m)
  {
    auto done = bufferQueue_->try_enqueue(std::move(m));
    if (!done) {
      // message dropped due to queue length
    } else if (isConnected_) {
      boost::asio::post(io_, [this, self = shared_from_this()](){
        // fetch all in bufferQueue
        std::vector<MessageUPtr> items(10);
        auto count = bufferQueue_->try_dequeue_bulk(items.begin(), 10);
        // put it in output Queue
        bool inProgress = !outputQueue_.empty();
        for (size_t i = 0; i < count; ++i) {
          outputQueue_.push(std::move(items[i]));
        }
        // do output
        if (!inProgress) {
          outputQueue_.front()->SerializeToString(&buffer_);
          sendMessage();
        }
      });
    }
  }
 private:
  void sendMessage()
  {
    boost::asio::async_write(socket_,
      boost::asio::buffer(buffer_.data(), buffer_.length()),
      [this, self = shared_from_this()](boost::system::error_code ec, size_t length)
      {
        if (!ec) {
          outputQueue_.pop();
          if (!outputQueue_.empty()) {
            outputQueue_.front()->SerializeToString(&buffer_);
            sendMessage();
          }
        } else {
          // do log
          socket_.close();
          // shutdown, remove this channel from sendChannels_;
        }
      });
  }
  void connect()
  {
    boost::asio::async_connect(
      socket_,
      nodeInfo_->endpoints,
      [this](boost::system::error_code ec, boost::asio::ip::tcp::endpoint endpoint)
      {
        if (ec) {
          // do log
          // shutdown, remove this channel from sendChannels_;
        } else {
          isConnected_ = true;
          if (!outputQueue_.empty()) {
            outputQueue_.front()->SerializeToString(&buffer_);
            sendMessage();
          }
        }
      });
  }
  std::atomic_bool isConnected_;
  boost::asio::io_context &io_;
  boost::asio::ip::tcp::socket socket_;
  NodeInfoSPtr nodeInfo_;
  BlockingConcurrentQueueSPtr<MessageUPtr> bufferQueue_;
  std::queue<MessageUPtr> outputQueue_;
  std::string buffer_;
};
using ChannelUPtr = std::unique_ptr<Channel>;

class Transport {
 public:
  std::unique_ptr<Transport> New(
    NodeHostConfigSPtr nhConfig,
    NodeAddressResolverSPtr resolver,
    RaftMessageHandlerSPtr handlers,
    std::function<std::string(uint64_t, uint64_t)> snapshotDirFunc);
  Transport();
  std::string name();
  //void setUnmanagedDeploymentID();
  void setDeploymentID(uint64_t id);

  // TODO: maybe just remove these interfaces, handle the message by Transport?
  //void setMessageHandler(RaftMessageHandler handler);
  //void removeMessageHandler();
  bool asyncSendMessage(MessageUPtr m)
  {
    std::shared_ptr<NodeInfo> node =
      resolver_->resolve(m->cluster_id(), m->to());
    if (node == nullptr) {
      log->warn(
        "{0} do not have the address for {1}:{2}, dropping a message",
        sourceAddress_, m->cluster_id(), m->to());
    }
    auto ch = sendChannels_.find(node->key);
    std::string test;
    if (ch == sendChannels_.end()) {
      sendChannels_[node->key] = std::make_unique<Channel>(io_, node, sendQueueLength_);
      //sendChannels_.insert({test, new Channel(io_, node, sendQueueLength_)});
      ch = sendChannels_.find(node->key);
    }
    ch->second->asyncSendMessage(std::move(m));
  }
  bool asyncSendSnapshot(MessageUPtr m);
  //std::shared_ptr<Sink> getStreamConnection(uint64_t clusterID, uint64_t nodeID);
  void start();
  void stop();
 private:
  slogger log;
  boost::asio::io_context io_;
  uint64_t streamConnections_;
  uint64_t sendQueueLength_;
  uint64_t getConnectedTimeoutS_;
  uint64_t idleTimeoutS_;

  uint64_t deploymentID;
  std::mutex mutex_;
  std::unordered_map<std::string, ChannelUPtr> sendChannels_; // GUARDED BY mutex_;
  BlockingConcurrentQueueUPtr<MessageBatchUPtr> outputQueue_;
  // std::unordered_map<std::string, CircuitBreaker> breakers_;
  uint32_t lanes_;
  // TransportMetrics metrics_;
  // server::Context serverCtx_;
  // config::NodeHostConfig nhConfig_;
  std::string sourceAddress_;
  NodeAddressResolverSPtr resolver_;
  RaftMessageHandlerSPtr handlers_;
};

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_TRANSPORT_H_
