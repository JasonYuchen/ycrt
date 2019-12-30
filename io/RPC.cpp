//
// Created by jason on 2019/12/24.
//

#include <string>
#include <vector>
#include <memory>
#include "settings/Hard.h"
#include "utils/Logger.h"
#include "RPC.h"

using std::string;
using std::vector;
using std::unique_ptr;
using std::shared_ptr;
using std::make_unique;
using std::make_shared;
using namespace boost::system;
using namespace boost::asio;

namespace ycrt
{

namespace io
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
using RequestHandler = std::function<void(std::unique_ptr<raftpb::MessageBatch>)>;
using ChunkHandler = std::function<void(std::unique_ptr<raftpb::SnapshotChunk>)>;

class ConnectionImpl : public std::enable_shared_from_this<ConnectionImpl> {
 public:
  explicit ConnectionImpl(io_context &io, RPC *rpc)
    : socket_(io), rpc_(rpc), payloadBuf_(PayloadBufSize) {}
  ip::tcp::socket &socket() {return socket_;}
  void setRequestHandlerPtr(RequestHandler *handler) { requestHandler_ = handler; }
  void setChunkHandlerPtr(ChunkHandler *handler) { chunkHandler_ = handler; }
  void start() {
    readHeader();
  }
 private:
  void readHeader() {
    async_read(
      socket_,
      buffer(headerBuf_, RequestHeaderSize),
      [this, self = shared_from_this()](error_code ec, size_t length)
      {
        if (!ec && decodeHeader()) {
          readPayload();
        } else {
          // warning
        }
      });
  }
  void readPayload() {
    async_read(socket_,
      buffer(payloadBuf_, header_.size),
      [this, self = shared_from_this()](error_code ec, size_t length)
      {
        if (!ec) {
          if (header_.method == RequestType) {
            auto msg = make_unique<raftpb::MessageBatch>();
            auto done = msg->ParseFromArray(payloadBuf_.data(), payloadBuf_.size());
            if (!done) {
              //rpc_->error();
              return;
            }
            (*requestHandler_)(std::move(msg));
          } else if (header_.method == SnapshotChunkType) {
            auto msg = make_unique<raftpb::SnapshotChunk>();
            auto done = msg->ParseFromArray(payloadBuf_.data(), payloadBuf_.size());
            if (!done) {
              //Log.get("transport")->error();
              return;
            }
            (*chunkHandler_)(std::move(msg));
          } else {
            //Log.get("transport")->
          }
          readHeader();
        } else {
          // warning;
        }
      });
  }
  bool decodeHeader() {

  }
  ip::tcp::socket socket_;
  RPC *rpc_;
  RequestHeader header_;
  char headerBuf_[RequestHeaderSize];
  vector<char> payloadBuf_;
  RequestHandler *requestHandler_;
  ChunkHandler *chunkHandler_;
};

void RPC::start()
{
  // tlsConfig = nhConfig_.getTLSConfig()
  auto pos = nhConifg_.ListenAddress.find(':');
  ip::tcp::endpoint endpoint(
    ip::make_address(nhConifg_.ListenAddress),
    std::stoi(nhConifg_.ListenAddress.substr(pos)));
  auto conn = std::make_shared<ConnectionImpl>(io_context_, this);
  acceptor_.async_accept(
    conn->socket(),
    [conn = std::move(conn), this](const error_code &error) mutable {
      if (error) {
        log->warn("accept error: {0}", error.message());
      } else {
        serveConnection(conn);
      }
      start();
    });
}

void RPC::serveConnection(shared_ptr<ConnectionImpl> &conn)
{
  conn->setRequestHandlerPtr(&requestHandler_);
  conn->setChunkHandlerPtr(&chunkHandler_);
  conn->start();
}

} // namespace io

} // namespace ycrt