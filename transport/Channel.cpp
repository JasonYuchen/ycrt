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

// TODO:
static StatusWith<string> loadSnapshotChunkData(const pbSnapshotChunk &chunk)
{
  try {
    auto file = SnapshotChunkFile::Open(
      chunk.filepath(), SnapshotChunkFile::READ);
    uint64_t offset = chunk.file_chunk_id() * settings::SnapshotChunkSize;
    string data;
    data.resize(chunk.chunk_size());
    file.ReadAt(data, offset).IsOKOrThrow();
    return data;
  } catch (Error &e) {
    Log.GetLogger("transport")->error(
      "failed to load chunk data due to {0}", e.what());
    return e.Code();
  }
}

// FIXME:
constexpr seconds ResolveDuration(1);
constexpr seconds ConnectDuration(1);
constexpr seconds SendDuration(1);
constexpr seconds ReadHeaderDuration(1);
constexpr seconds ReadPayloadDuration(1);

SendChannel::SendChannel(
  Transport &transport,
  io_context &io,
  string source,
  NodesRecordSPtr nodeRecord,
  uint64_t queueLen)
  : log(Log.GetLogger("transport")),
    transport_(transport),
    isConnected_(false),
    inQueue_(false),
    sourceAddress_(std::move(source)),
    io_(io),
    socket_(io),
    resolver_(io),
    idleTimer_(io),
    stopped_(false),
    nodeRecord_(std::move(nodeRecord)),
    bufferQueue_(make_shared<BlockingConcurrentQueue<pbMessageUPtr>>(queueLen))
{
  buffer_.reserve(RequestHeaderSize);
}

void SendChannel::Start()
{
  resolve();
  checkIdle();
}

// one channel per remote raft node,
// asyncSendMessage of each SendChannel will only be called in one thread
bool SendChannel::AsyncSendMessage(pbMessageUPtr m)
{
  auto done = bufferQueue_->try_enqueue(std::move(m));
  if (!done) {
    // message dropped due to queue length
    log->warn("message dropped due to queue size");
    return false;
  } else if (isConnected_ && !inQueue_.exchange(true)) {
    // inQueue to prevent too many pending posted callbacks
    asyncSendMessage();
  }
  return true;
}

SendChannel::~SendChannel()
{
}

void SendChannel::prepareBuffer()
{
  // FIXME: do not hack
  RequestHeader header{RequestType, 0, 0};
  buffer_.clear();
  buffer_.insert(0, RequestHeaderSize, 0);
  header.Encode(const_cast<char *>(buffer_.data()), RequestHeaderSize);
  outputQueue_.front()->AppendToString(&buffer_);
  uint64_t total = buffer_.size() - RequestHeaderSize;
  ::memcpy(const_cast<char *>(buffer_.data()+8), &total, sizeof(total));
}

void SendChannel::asyncSendMessage()
{
  idleTimer_.expires_from_now(seconds(1));
  boost::asio::post(io_, [this, self = shared_from_this()](){
    inQueue_ = false;
    // FIXME: fetch all in bufferQueue, 10 ?
    vector<pbMessageUPtr> items(10);
    while (true) {
      items.clear();
      auto count = bufferQueue_->try_dequeue_bulk(items.begin(), 10);
      if (count == 0) {
        return;
      }
      // put it in output Queue
      bool inProgress = !outputQueue_.empty();
      auto batch = make_unique<raftpb::MessageBatch>();
      batch->set_source_address(sourceAddress_);
      batch->set_deployment_id(transport_.GetDeploymentID());
      // TODO: MessageBatch rpc bin ver
      for (size_t i = 0; i < count; ++i) {
        // TODO: use customized serialization, not thread-safe to use move here !!!
        batch->mutable_requests()->AddAllocated(items[i].release());
      }
      outputQueue_.push(std::move(batch));
      // do output
      if (!inProgress) {
        log->debug("SendChannel to {0} with next message {1}",
          nodeRecord_->Address, outputQueue_.front()->DebugString());
        sendMessage();
      }
    }
  });
}

void SendChannel::sendMessage()
{
  idleTimer_.expires_from_now(SendDuration);
  prepareBuffer();
  log->debug("SendChannel send {0} bytes to {1}", buffer_.length(), nodeRecord_->Address);
  boost::asio::async_write(socket_,
    buffer(buffer_.data(), buffer_.length()),
    [this, self = shared_from_this()](error_code ec, size_t length)
    {
      if (!ec) {
        outputQueue_.pop();
        if (!outputQueue_.empty()) {
          log->debug("SendChannel to {0} with next message {1}",
            nodeRecord_->Address, outputQueue_.front()->DebugString());
          sendMessage();
        }
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        // FIXME: do log, nodeInfo_->key ?
        log->warn("SendChannel to {0} closed due to async_write error {1}",
          nodeRecord_->Address, ec.message());
        transport_.RemoveSendChannel(nodeRecord_->Key);
        stop();
      }
    });
}

void SendChannel::resolve()
{
  idleTimer_.expires_from_now(ResolveDuration);
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
        transport_.RemoveSendChannel(nodeRecord_->Key);
        stop();
      }
    });
}

void SendChannel::connect(tcp::resolver::results_type endpointIter)
{
  idleTimer_.expires_from_now(ConnectDuration);
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
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->warn("SendChannel to {0} closed due to async_connect error {1}",
          nodeRecord_->Address, ec.message());
        transport_.RemoveSendChannel(nodeRecord_->Key);
        stop();
      }
    });
}

void SendChannel::checkIdle()
{
  idleTimer_.async_wait(
    [this, self = shared_from_this()] (error_code ec)
    {
      if (stopped_) {
        return;
      }
      if (idleTimer_.expiry() <= steady_timer::clock_type::now()) {
        // the deadline has passed
        stop();
        log->warn("send channel for {0} timed out", nodeRecord_->Key);
      } else {
        // the deadline has not passed, wait again
        checkIdle();
      }
    });
}

void SendChannel::stop()
{
  if (!stopped_) {
    transport_.HandleUnreachable(nodeRecord_->Address);
    stopped_ = true;
    socket_.close();
    idleTimer_.cancel();
  }
}

RecvChannel::RecvChannel(Transport &tranport, io_context &io)
  : log(Log.GetLogger("transport")),
    transport_(tranport),
    socket_(io),
    idleTimer_(io),
    stopped_(false),
    payloadBuf_(PayloadBufSize)
{
}

void RecvChannel::Start()
{
  readHeader();
  checkIdle();
}

RecvChannel::~RecvChannel()
{
}

void RecvChannel::readHeader()
{
  idleTimer_.expires_from_now(ReadHeaderDuration);
  boost::asio::async_read(
    socket_,
    buffer(headerBuf_, RequestHeaderSize),
    [this, self = shared_from_this()](error_code ec, size_t length)
    {
      if (!ec) {
        if (decodeHeader()) {
          readPayload();
        } else {
          log->error("RecvChannel closed due to invalid header");
          stop();
          return;
        }
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->error("RecvChannel closed due to async_read error {0}",
          ec.message());
        stop();
        return;
      }
    });
}

void RecvChannel::readPayload()
{
  idleTimer_.expires_from_now(ReadPayloadDuration);
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
            log->error("RecvChannel closed due to invalid MessageBatch");
            stop();
            return;
          }
          if (!transport_.HandleRequest(std::move(msg))) {
            log->error("RecvChannel closed due to request rejected by handler");
            stop();
            return;
          }
        } else if (header_.Method == SnapshotChunkType) {
          auto msg = make_unique<raftpb::SnapshotChunk>();
          auto done = msg->ParseFromArray(payloadBuf_.data(), header_.Size);
          if (!done) {
            log->error("RecvChannel closed due to invalid SnapshotChunk");
            stop();
            return;
          }
          if (!transport_.HandleSnapshotChunk(std::move(msg))) {
            log->error("RecvChannel closed due to chunk rejected by handler");
            stop();
            return;
          }
        } else {
          // should not reach here
          log->error("RecvChannel closed due to invalid method type");
          stop();
          return;
        }
        readHeader();
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->error("RecvChannel closed due to error {0}", ec.message());
        stop();
        return;
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

void RecvChannel::checkIdle()
{
  idleTimer_.async_wait(
    [this, self = shared_from_this()] (error_code ec)
    {
      if (stopped_) {
        return;
      }
      if (idleTimer_.expiry() <= steady_timer::clock_type::now()) {
        // the deadline has passed
        stop();
        log->warn("receive channel for {0} timed out", socket_.remote_endpoint().address().to_string());
      } else {
        // the deadline has not passed, wait again
        checkIdle();
      }
    });
}

void RecvChannel::stop()
{
  if (!stopped_) {
    stopped_ = true;
    socket_.close();
    idleTimer_.cancel();
  }
}

SnapshotLane::SnapshotLane(
  Transport &transport,
  atomic_uint64_t &laneCount,
  io_context &io,
  string source,
  NodesRecordSPtr nodeRecord,
  NodeInfo node)
  : log(Log.GetLogger("transport")),
    transport_(transport),
    laneCount_(laneCount),
    sourceAddress_(std::move(source)),
    io_(io),
    socket_(io),
    resolver_(io),
    idleTimer_(io),
    stopped_(false),
    rejected_(true),
    nodeRecord_(std::move(nodeRecord)),
    node_(node)
{
  buffer_.reserve(RequestHeaderSize + settings::SnapshotChunkSize);
  laneCount_++;
}

void SnapshotLane::Start(std::vector<pbSnapshotChunkSPtr> &&savedChunks)
{
  uint64_t id = 0;
  for (auto &chunk : savedChunks) {
    if (chunk->chunk_id() != id++) {
      throw Error(ErrorCode::UnexpectedRaftMessage, log, "gap found in savedChunks");
    }
    outputQueue_.push(std::move(chunk));
  }
  total_ = id;
  resolve();
  checkIdle();
}

SnapshotLane::~SnapshotLane()
{
  laneCount_--;
}

void SnapshotLane::prepareBuffer()
{
  // FIXME: do not hack
  RequestHeader header{SnapshotChunkType, 0, 0};
  buffer_.clear();
  buffer_.insert(0, RequestHeaderSize, 0);
  header.Encode(const_cast<char *>(buffer_.data()), RequestHeaderSize);
  // FIXME: load file chunk to chunk->data
  outputQueue_.front()->set_deployment_id(transport_.GetDeploymentID());
  if (!outputQueue_.front()->witness()) {
    StatusWith<string> data = loadSnapshotChunkData(*outputQueue_.front());
    if (!data.IsOK()) {
      stop();
      return;
    }
    outputQueue_.front()->set_data(std::move(data.GetMutableOrThrow()));
  }
  outputQueue_.front()->AppendToString(&buffer_);
  uint64_t total = buffer_.size() - RequestHeaderSize;
  ::memcpy(const_cast<char *>(buffer_.data()+8), &total, sizeof(total));
}

void SnapshotLane::sendMessage()
{
  idleTimer_.expires_from_now(SendDuration);
  prepareBuffer();
  log->info("SnapshotLane is sending chunk {0}/{1} to {2}", outputQueue_.front()->chunk_id() + 1, total_, nodeRecord_->Address);
  boost::asio::async_write(socket_,
    buffer(buffer_.data(), buffer_.length()),
    [this, self = shared_from_this()](error_code ec, size_t length)
    {
      if (!ec) {
        outputQueue_.pop();
        if (!outputQueue_.empty()) {
          log->debug("SnapshotLane to {0} with next message {1}",
            nodeRecord_->Address, outputQueue_.front()->DebugString());
          sendMessage();
        } else {
          log->info("SnapshotLane succeeded sending a snapshot with {0} chunks"
                    " to {1} {2}", total_, nodeRecord_->Address,
                    FmtClusterNode(node_.ClusterID, node_.NodeID));
          rejected_ = false;
          stop();
        }
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        // FIXME: do log, nodeInfo_->key ?
        log->warn("SnapshotLane to {0} closed due to async_write error {1}",
          nodeRecord_->Address, ec.message());
        transport_.RemoveSendChannel(nodeRecord_->Key);
        stop();
      }
    });
}

void SnapshotLane::resolve()
{
  idleTimer_.expires_from_now(ResolveDuration);
  resolver_.async_resolve(getEndpoint(string_view(nodeRecord_->Address)),
    [this, self = shared_from_this()]
      (error_code ec, tcp::resolver::results_type it) {
      if (!ec) {
        log->debug("remote endpoint resolved, connect to {0}", it->host_name());
        connect(it);
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->warn("SnapshotLane to {0} closed due to async_resolve error {1}",
          nodeRecord_->Address, ec.message());
        stop();
      }
    });
}

void SnapshotLane::connect(tcp::resolver::results_type endpointIter)
{
  idleTimer_.expires_from_now(ConnectDuration);
  boost::asio::async_connect(
    socket_,
    endpointIter,
    [this, self = shared_from_this()](error_code ec, tcp::endpoint endpoint)
    {
      log->debug("SnapshotLane connect to {0} returned {1}",
        endpoint.address().to_string(), ec.message());
      if (!ec) {
        log->info("SnapshotLane connect to {0}", endpoint.address().to_string());
        prepareBuffer();
        sendMessage();
      } else if (ec.value() == error::operation_aborted) {
        return;
      } else {
        log->warn("SnapshotLane to {0} closed due to async_connect error {1}",
          nodeRecord_->Address, ec.message());
        stop();
      }
    });
}

void SnapshotLane::checkIdle()
{
  idleTimer_.async_wait(
    [this, self = shared_from_this()] (error_code ec)
    {
      if (stopped_) {
        return;
      }
      if (idleTimer_.expiry() <= steady_timer::clock_type::now()) {
        // the deadline has passed
        stop();
        log->warn("send channel for {0} timed out", nodeRecord_->Key);
      } else {
        // the deadline has not passed, wait again
        checkIdle();
      }
    });
}

void SnapshotLane::stop()
{
  if (!stopped_) {
    transport_.SendSnapshotNotification(node_, rejected_);
    stopped_ = true;
    socket_.close();
    idleTimer_.cancel();
  }
}

} // namespace transport

} // namespace ycrt