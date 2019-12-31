//
// Created by jason on 2019/12/21.
//

#include "Channel.h"
#include "Transport.h"

using namespace std;
using namespace boost::asio;
using namespace boost::asio::chrono;

namespace ycrt
{

namespace transport
{

SendChannel::SendChannel(
  Transport *tranport,
  io_context &io,
  string source,
  NodeInfoSPtr node,
  uint64_t queueLen)
  : transport_(tranport),
    log(Log.get("transport")),
    isConnected_(false),
    inQueue_(false),
    sourceAddress_(std::move(source)),
    io_(io),
    socket_(io),
    nodeInfo_(std::move(node)),
    bufferQueue_(make_shared<BlockingConcurrentQueue<MessageUPtr>>(queueLen))
{
  connect();
}

// one channel per remote raft node,
// asyncSendMessage of each SendChannel will only be called in one thread
bool SendChannel::asyncSendMessage(MessageUPtr m)
{
  auto done = bufferQueue_->try_enqueue(std::move(m));
  if (!done) {
    // message dropped due to queue length
    log->warn("message dropped due to queue size");
  } else if (isConnected_ && !inQueue_.exchange(true)) {
    // inQueue to prevent too many pending posted callbacks
    boost::asio::post(io_, [this, self = shared_from_this()](){
      inQueue_ = false;
      // fetch all in bufferQueue, 10 ?
      vector<MessageUPtr> items(10);
      auto count = bufferQueue_->try_dequeue_bulk(items.begin(), 10);
      if (count == 0) {
        return;
      }
      // put it in output Queue
      bool inProgress = !outputQueue_.empty();
      auto batch = make_unique<raftpb::MessageBatch>();
      batch->set_source_address(sourceAddress_);
      // TODO: MessageBatch rpc bin ver
      for (size_t i = 0; i < count; ++i) {
        batch->mutable_requests()->AddAllocated(items[i].release());
      }
      outputQueue_.push(std::move(batch));
      // do output
      if (!inProgress) {
        log->debug("SendChannel to {0} with next message {1}", nodeInfo_->key, outputQueue_.front()->DebugString());
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
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        // FIXME: do log, nodeInfo_->key ?
        log->warn("SendChannel to {0} closed due to async_write error {1}",
          nodeInfo_->key, ec.message());
        transport_->removeSendChannel(nodeInfo_->key);
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
    [this, self = shared_from_this()](error_code ec, ip::tcp::endpoint endpoint)
    {
      log->debug("SendChannel connect to {0} returned {1}", endpoint.address().to_string(), ec.message());
      if (!ec) {
        isConnected_ = true;
        if (!outputQueue_.empty()) {
          outputQueue_.front()->SerializeToString(&buffer_);
          sendMessage();
        }
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        // do log
        log->warn("SendChannel to {0} closed due to async_connect error {1}",
          nodeInfo_->key, ec.message());
        transport_->removeSendChannel(nodeInfo_->key);
        socket_.close();
        // shutdown, remove this channel from sendChannels_;
      }
    });
}

RecvChannel::RecvChannel(Transport *tranport, io_context &io)
  : transport_(tranport),
    log(Log.get("transport")),
    socket_(io),
    payloadBuf_(PayloadBufSize)
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
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        if (ec) {
          log->warn("");
        } else {

        }
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
      } else if (ec.value() == error::operation_aborted) {
        return;
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