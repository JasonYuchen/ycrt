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
#include "ycrt/Config.h"
#include "Nodes.h"

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
    RequestHeader header{};
    ::memcpy(&header.method, &buf[0], 4);
    ::memcpy(&header.crc32, &buf[4], 4);
    ::memcpy(&header.size, &buf[8], 8);
    return header;
  }
};

static_assert(RequestHeaderSize == sizeof(RequestHeader),
  "RequestHeaderSize != 16");

class Transport;
class SendChannel : public std::enable_shared_from_this<SendChannel> {
 public:
  explicit SendChannel(
    Transport *tranport,
    boost::asio::io_context &io,
    std::string source,
    NodesRecordSPtr nodeRecord,
    uint64_t queueLength);
  bool asyncSendMessage(MessageUPtr m);
  ~SendChannel();
 private:
  void sendMessage();
  void resolve();
  void connect(boost::asio::ip::tcp::resolver::results_type endpointIter);
  slogger log;
  Transport *transport_;
  std::atomic_bool isConnected_;
  std::atomic_bool inQueue_;
  std::string sourceAddress_;
  boost::asio::io_context &io_;
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::resolver resolver_;
  NodesRecordSPtr nodeRecord_;
  BlockingConcurrentQueueSPtr<MessageUPtr> bufferQueue_;
  std::queue<MessageBatchUPtr> outputQueue_;
  std::string buffer_;
};
using SendChannelSPtr = std::shared_ptr<SendChannel>;
using SendChannelUPtr = std::unique_ptr<SendChannel>;

using RequestHandler = std::function<void(MessageBatchUPtr)>;
using ChunkHandler = std::function<void(SnapshotChunkUPtr)>;
class RecvChannel : public std::enable_shared_from_this<RecvChannel> {
 public:
  explicit RecvChannel(Transport *tranport, boost::asio::io_context &io);
  boost::asio::ip::tcp::socket &socket() {return socket_;}
  void setRequestHandlerPtr(RequestHandler &&handler)
  {
    requestHandler_ = std::move(handler);
  }
  void setChunkHandlerPtr(ChunkHandler &&handler)
  {
    chunkHandler_ = std::move(handler);
  }
  void start();
  ~RecvChannel();
 private:
  void readHeader();
  void readPayload();
  bool decodeHeader();
  slogger log;
  Transport *transport_;
  boost::asio::ip::tcp::socket socket_;
  RequestHeader header_;
  char headerBuf_[RequestHeaderSize];
  std::vector<char> payloadBuf_;
  RequestHandler requestHandler_;
  ChunkHandler chunkHandler_;
};
using RecvChannelSPtr = std::shared_ptr<RecvChannel>;
using RecvChannelUPtr = std::unique_ptr<RecvChannel>;

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_CHANNEL_H_
