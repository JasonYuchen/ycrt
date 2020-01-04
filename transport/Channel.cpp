//
// Created by jason on 2019/12/21.
//

#include "Channel.h"
#include "Transport.h"

using namespace std;
using namespace boost::asio;
using namespace boost::asio::chrono;
using namespace boost::asio::ip;

namespace ycrt
{

namespace transport
{

SendChannel::SendChannel(
  Transport *tranport,
  io_context &io,
  string source,
  NodesRecordSPtr nodeRecord,
  uint64_t queueLen)
  : transport_(tranport),
    log(Log.GetLogger("transport")),
    isConnected_(false),
    inQueue_(false),
    sourceAddress_(std::move(source)),
    io_(io),
    socket_(io),
    resolver_(io),
    nodeRecord_(std::move(nodeRecord)),
    bufferQueue_(make_shared<BlockingConcurrentQueue<pbMessageSPtr>>(queueLen))
{
}

void SendChannel::Start()
{
  resolve();
}

// one channel per remote raft node,
// asyncSendMessage of each SendChannel will only be called in one thread
bool SendChannel::AsyncSendMessage(pbMessageSPtr m)
{
  auto done = bufferQueue_->try_enqueue(std::move(m));
  if (!done) {
    // message dropped due to queue length
    log->warn("message dropped due to queue size");
  } else if (isConnected_ && !inQueue_.exchange(true)) {
    // inQueue to prevent too many pending posted callbacks
    asyncSendMessage();
  }
}

SendChannel::~SendChannel()
{
}

void SendChannel::asyncSendMessage()
{
  boost::asio::post(io_, [this, self = shared_from_this()](){
    inQueue_ = false;
    // fetch all in bufferQueue, 10 ?
    vector<pbMessageSPtr> items(10);
    auto count = bufferQueue_->try_dequeue_bulk(items.begin(), 10);
    if (count == 0) {
      return;
    }
    // put it in output Queue
    bool inProgress = !outputQueue_.empty();
    auto batch = make_unique<raftpb::MessageBatch>();
    batch->set_source_address(sourceAddress_);
    batch->set_deployment_id(transport_->GetDeploymentID());
    // TODO: MessageBatch rpc bin ver
    for (size_t i = 0; i < count; ++i) {
      // TODO: use customized serialization, not thread-safe to use move here !!!
      batch->mutable_requests()->Add(std::move(*items[i]));
    }
    outputQueue_.push(std::move(batch));
    // do output
    if (!inProgress) {
      log->debug("SendChannel to {0} with next message {1}",
        nodeRecord_->Address, outputQueue_.front()->DebugString());
      outputQueue_.front()->SerializeToString(&buffer_);
      RequestHeader header{RequestType, 0, buffer_.size()};
      header.Encode(headerBuf_, RequestHeaderSize);
      buffer_ = std::string(headerBuf_, RequestHeaderSize) + buffer_;
      sendMessage();
    }
  });
}

void SendChannel::sendMessage()
{
  log->debug("SendChannel send {0} bytes to {1}", buffer_.length(), nodeRecord_->Address);
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
          nodeRecord_->Address, ec.message());
        transport_->RemoveSendChannel(nodeRecord_->Key);
        socket_.close();
        // shutdown, remove this channel from sendChannels_;
      }
    });
}

void SendChannel::resolve()
{
  resolver_.async_resolve(getEndpoint(string_view(nodeRecord_->Address)),
    [this, self = shared_from_this()]
    (error_code ec, tcp::resolver::results_type it) {
      if (!ec) {
        log->debug("remote endpoint resolved, connect to {0}", it->host_name());
        connect(it);
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->warn("SendChannel to {0} closed due to async_resolve error {1}",
          nodeRecord_->Address, ec.message());
        transport_->RemoveSendChannel(nodeRecord_->Key);
        socket_.close();
      }
    });
}

void SendChannel::connect(tcp::resolver::results_type endpointIter)
{
  boost::asio::async_connect(
    socket_,
    endpointIter,
    [this, self = shared_from_this()](error_code ec, tcp::endpoint endpoint)
    {
      log->debug("SendChannel connect to {0} returned {1}",
        endpoint.address().to_string(), ec.message());
      if (!ec) {
        log->info("SendChannel connect to {0}", endpoint.address().to_string());
        isConnected_ = true;
        asyncSendMessage();
//        if (!outputQueue_.empty()) {
//          outputQueue_.front()->SerializeToString(&buffer_);
//          sendMessage();
//        }
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        // do log
        log->warn("SendChannel to {0} closed due to async_connect error {1}",
          nodeRecord_->Address, ec.message());
        transport_->RemoveSendChannel(nodeRecord_->Key);
        socket_.close();
        // shutdown, remove this channel from sendChannels_;
      }
    });
}

RecvChannel::RecvChannel(Transport *tranport, io_context &io)
  : transport_(tranport),
    log(Log.GetLogger("transport")),
    socket_(io),
    payloadBuf_(PayloadBufSize)
{
}

void RecvChannel::Start()
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
          log->error("RecvChannel closed due to async_read error {0}",
            ec.message());
        } else {
          log->error("RecvChannel closed due to invalid header");
        }
        socket_.close();
      }
    });
}

void RecvChannel::readPayload()
{
  boost::asio::async_read(socket_,
    buffer(payloadBuf_, header_.Size),
    [this, self = shared_from_this()](error_code ec, size_t length)
    {
      if (!ec) {
        // FIXME: check crc32
        if (header_.Method == RequestType) {
          auto msg = make_unique<raftpb::MessageBatch>();
          auto done = msg->ParseFromArray(payloadBuf_.data(), header_.Size);
          if (!done) {
            //rpc_->error();
            return;
          }
          requestHandler_(std::move(msg));
        } else if (header_.Method == SnapshotChunkType) {
          auto msg = make_unique<raftpb::SnapshotChunk>();
          auto done = msg->ParseFromArray(payloadBuf_.data(), header_.Size);
          if (!done) {
            //Log.get("transport")->error();
            return;
          }
          chunkHandler_(std::move(msg));
        } else {
          // should not reach here
          log->error("RecvChannel closed due to invalid method type");
          socket_.close();
          return;
        }
        readHeader();
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->error("RecvChannel closed due to async_read error {0}",
          ec.message());
        socket_.close();
      }
    });
}

bool RecvChannel::decodeHeader()
{
  header_ = RequestHeader::Decode(headerBuf_, RequestHeaderSize);
  if (header_.Method != RequestType && header_.Method != SnapshotChunkType) {
    log->error("invalid method");
    return false;
  }
  if (header_.Size == 0) {
    log->error("invalid payload size");
    return false;
  }
  if (header_.Size > payloadBuf_.size()) {
    payloadBuf_.resize(header_.Size);
  }
  // FIXME: check crc32
  return true;
}

} // namespace transport

} // namespace ycrt