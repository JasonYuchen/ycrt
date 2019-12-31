//
// Created by jason on 2019/12/21.
//

#include "Channel.h"

using namespace std;
using namespace boost::asio;

namespace ycrt
{

namespace transport
{

SendChannel::SendChannel(io_context &io, NodeInfoSPtr node, uint64_t queueLen)
  : io_(io),
    socket_(io),
    nodeInfo_(std::move(node)),
    bufferQueue_(make_shared<BlockingConcurrentQueue<MessageUPtr>>(queueLen))
{
  connect();
}

bool SendChannel::asyncSendMessage(MessageUPtr m)
{
  auto done = bufferQueue_->try_enqueue(std::move(m));
  if (!done) {
    // message dropped due to queue length
  } else if (isConnected_) {
    boost::asio::post(io_, [this, self = shared_from_this()](){
      // fetch all in bufferQueue
      vector<MessageUPtr> items(10);
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

SendChannel::~SendChannel()
{
}

void SendChannel::sendMessage()
{
  boost::asio::async_write(socket_,
    buffer(buffer_.data(), buffer_.length()),
    [this, self = shared_from_this()](error_code ec, size_t length)
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

void SendChannel::connect()
{
  boost::asio::async_connect(
    socket_,
    nodeInfo_->endpoints,
    [this](error_code ec, ip::tcp::endpoint endpoint)
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

RecvChannel::RecvChannel(io_context &io)
  : socket_(io), payloadBuf_(PayloadBufSize)
{
}

void RecvChannel::start()
{
  readHeader();
}

RecvChannel::~RecvChannel()
{
}

void RecvChannel::readHeader()
{
  boost::asio::async_read(
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

void RecvChannel::readPayload()
{
  boost::asio::async_read(socket_,
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
          requestHandler_(std::move(msg));
        } else if (header_.method == SnapshotChunkType) {
          auto msg = make_unique<raftpb::SnapshotChunk>();
          auto done = msg->ParseFromArray(payloadBuf_.data(), payloadBuf_.size());
          if (!done) {
            //Log.get("transport")->error();
            return;
          }
          chunkHandler_(std::move(msg));
        } else {
          //Log.get("transport")->
        }
        readHeader();
      } else {
        // warning;
      }
    });
}

bool RecvChannel::decodeHeader()
{
  // FIXME
  return true;
}

} // namespace transport

} // namespace ycrt