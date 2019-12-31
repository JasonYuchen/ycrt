//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_TRANSPORT_CHANNEL_H_
#define YCRT_TRANSPORT_CHANNEL_H_

#include <queue>
#include <memory>
#include <boost/asio.hpp>
#include "settings/Hard.h"
#include "pb/RaftMessage.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace transport
{

constexpr uint64_t RequestHeaderSize = 16;
constexpr uint64_t PayloadBufSize = settings::SnapshotChunkSize + 1024 * 128;
constexpr uint32_t RequestType = 100;
constexpr uint32_t SnapshotChunkType = 200;

struct RequestHeader {
  uint32_t method;
  uint32_t crc32;
  uint64_t size;
  // FIXME
  void encode(char *buf, size_t len) {
    ::memcpy(&buf[0], &method, 4);
    ::memcpy(&buf[4], &crc32, 4);
    ::memcpy(&buf[8], &size, 8);
  }
  // FIXME
  static RequestHeader decode(const char *buf, size_t len) {

  }
};

static_assert(RequestHeaderSize == sizeof(RequestHeader), "RequestHeaderSize != 16");

class NodeInfo {
 public:
  std::string key;
  boost::asio::ip::tcp::resolver::results_type endpoints;
};
using NodeInfoSPtr = std::shared_ptr<NodeInfo>;

class NodeAddressResolver {
 public:
  NodeInfoSPtr resolve(uint64_t clusterID, uint64_t nodeID);
  void addRemoteAddress(uint64_t clusterID, uint64_t nodeID, std::string address);
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

class SendChannel : public std::enable_shared_from_this<SendChannel> {
 public:
  explicit SendChannel(boost::asio::io_context &io, NodeInfoSPtr node, uint64_t queueLength);
  bool asyncSendMessage(MessageUPtr m);
  ~SendChannel();
 private:
  void sendMessage();
  void connect();
  std::atomic_bool isConnected_;
  boost::asio::io_context &io_;
  boost::asio::ip::tcp::socket socket_;
  NodeInfoSPtr nodeInfo_;
  BlockingConcurrentQueueSPtr<MessageUPtr> bufferQueue_;
  std::queue<MessageUPtr> outputQueue_;
  std::string buffer_;
};
using SendChannelUPtr = std::unique_ptr<SendChannel>;

using RequestHandler = std::function<void(MessageBatchUPtr)>;
using ChunkHandler = std::function<void(SnapshotChunkUPtr)>;
class RecvChannel : public std::enable_shared_from_this<RecvChannel> {
 public:
  explicit RecvChannel(boost::asio::io_context &io);
  boost::asio::ip::tcp::socket &socket() {return socket_;}
  void setRequestHandlerPtr(RequestHandler handler) { requestHandler_ = std::move(handler); }
  void setChunkHandlerPtr(ChunkHandler handler) { chunkHandler_ = std::move(handler); }
  void start();
  ~RecvChannel();
 private:
  void readHeader();
  void readPayload();
  bool decodeHeader();
  boost::asio::ip::tcp::socket socket_;
  RequestHeader header_;
  char headerBuf_[RequestHeaderSize];
  std::vector<char> payloadBuf_;
  RequestHandler requestHandler_;
  ChunkHandler chunkHandler_;
};

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_CHANNEL_H_
