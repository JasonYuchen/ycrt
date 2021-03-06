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
#include "NodeResolver.h"

namespace ycrt
{

namespace transport
{

// TODO: move RequestHeader etc to pb/RaftMessage.h
constexpr uint64_t RequestHeaderSize = 16;
constexpr uint64_t PayloadBufSize = settings::SnapshotChunkSize + 1024 * 128;
constexpr uint32_t RequestType = 100;
constexpr uint32_t SnapshotChunkType = 200;

struct RequestHeader {
  uint32_t Method;
  uint32_t CRC32;
  uint64_t Size;
  // FIXME
  void Encode(char *buf, size_t len) {
    ::memcpy(&buf[0], &Method, 4);
    ::memcpy(&buf[4], &CRC32, 4);
    ::memcpy(&buf[8], &Size, 8);
  }
  // FIXME
  static RequestHeader Decode(const char *buf, size_t len) {
    RequestHeader header{};
    ::memcpy(&header.Method, &buf[0], 4);
    ::memcpy(&header.CRC32, &buf[4], 4);
    ::memcpy(&header.Size, &buf[8], 8);
    return header;
  }
};

static_assert(RequestHeaderSize == sizeof(RequestHeader),
  "RequestHeaderSize != 16");

class Transport;
class SendChannel : public std::enable_shared_from_this<SendChannel> {
 public:
  explicit SendChannel(
    Transport &transport,
    boost::asio::io_context &io,
    std::string source,
    NodesRecordSPtr nodeRecord,
    uint64_t queueLength);
  void Start();
  bool AsyncSendMessage(pbMessageUPtr m);
  ~SendChannel();
 private:
  void prepareBuffer();
  void asyncSendMessage();
  void sendMessage();
  void resolve();
  void connect(boost::asio::ip::tcp::resolver::results_type endpointIter);
  void checkIdle();
  void stop();
  slogger log;
  Transport &transport_;
  std::atomic_bool isConnected_;
  std::atomic_bool inQueue_;
  std::string sourceAddress_;
  boost::asio::io_context &io_;
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::resolver resolver_;
  boost::asio::steady_timer idleTimer_;
  bool stopped_;
  NodesRecordSPtr nodeRecord_;
  BlockingConcurrentQueueSPtr<pbMessageUPtr> bufferQueue_;
  std::queue<pbMessageBatchUPtr> outputQueue_;
  std::string buffer_;
};
using SendChannelSPtr = std::shared_ptr<SendChannel>;

class RecvChannel : public std::enable_shared_from_this<RecvChannel> {
 public:
  explicit RecvChannel(Transport &tranport, boost::asio::io_context &io);
  boost::asio::ip::tcp::socket &socket() {return socket_;}
  void Start();
  ~RecvChannel();
 private:
  void readHeader();
  void readPayload();
  bool decodeHeader();
  void checkIdle();
  void stop();
  slogger log;
  Transport &transport_;
  std::string remoteAddress_;
  boost::asio::ip::tcp::socket socket_;
  boost::asio::steady_timer idleTimer_;
  bool stopped_;
  RequestHeader header_;
  char headerBuf_[RequestHeaderSize];
  std::vector<char> payloadBuf_;
};
using RecvChannelSPtr = std::shared_ptr<RecvChannel>;

class SnapshotLane : public std::enable_shared_from_this<SnapshotLane> {
 public:
  explicit SnapshotLane(
    Transport &transport,
    std::atomic_uint64_t &laneCount,
    boost::asio::io_context &io,
    std::string source,
    NodesRecordSPtr nodeRecord,
    NodeInfo node);
  void Start(std::vector<pbSnapshotChunkSPtr> &&savedChunks);
  ~SnapshotLane();
 private:
  void prepareBuffer();
  void sendMessage();
  void resolve();
  void connect(boost::asio::ip::tcp::resolver::results_type endpointIter);
  void checkIdle();
  void stop();
  slogger log;
  Transport &transport_;
  std::atomic_uint64_t &laneCount_;
  std::string sourceAddress_;
  boost::asio::io_context &io_;
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::resolver resolver_;
  boost::asio::steady_timer idleTimer_;
  bool stopped_;
  bool rejected_;
  uint64_t total_;
  NodesRecordSPtr nodeRecord_;
  NodeInfo node_;
  std::queue<pbSnapshotChunkSPtr> outputQueue_;
  std::string buffer_;
};
using SnapshotLaneSPtr = std::shared_ptr<SnapshotLane>;

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_CHANNEL_H_
