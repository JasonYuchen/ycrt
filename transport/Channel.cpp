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

// TODO: move to pbSnapshotChunk
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
      "loadSnapshotChunkData: failed to load chunk data due to {}", e.what());
    return e.Code();
  }
}

// TODO: move to the settings/Soft.h
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
  log->info("SendChannel::Start: channel {}", nodeRecord_->Key);
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
    log->warn("SendChannel::AsyncSendMessage: exceeds queue size, dropped");
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
    constexpr int batchSize = 10;
    vector<pbMessageUPtr> items(batchSize);
    while (true) {
      items.clear();
      auto count = bufferQueue_->try_dequeue_bulk(items.begin(), batchSize);
      if (count == 0) {
        return;
      }
      bool inProgress = !outputQueue_.empty();
      auto batch = make_unique<raftpb::MessageBatch>();
      batch->set_source_address(sourceAddress_);
      batch->set_deployment_id(transport_.GetDeploymentID());
      for (size_t i = 0; i < count; ++i) {
        // FIXME: use customized serialization, not thread-safe to use move here !!!
        batch->mutable_requests()->AddAllocated(items[i].release());
      }
      outputQueue_.push(std::move(batch));
      // do output
      if (!inProgress) {
        log->debug("SendChannel::asyncSendMessage: send to {} with message: {}",
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
  log->debug("SendChannel::sendMessage: send {} bytes to {}",
    buffer_.length(), nodeRecord_->Address);
  boost::asio::async_write(socket_,
    buffer(buffer_.data(), buffer_.length()),
    [this, self = shared_from_this()](error_code ec, size_t length)
    {
      if (!ec) {
        outputQueue_.pop();
        if (!outputQueue_.empty()) {
          log->debug("SendChannel::sendMessage: send to {} with message: {}",
            nodeRecord_->Address, outputQueue_.front()->DebugString());
          sendMessage();
        }
      } else if (ec.value() == error::operation_aborted) {
        log->debug("SendChannel::sendMessage: operation aborted");
        return;
      } else {
        log->warn("SendChannel::sendMessage: async_write error: {}",
          ec.message());
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
        log->debug("SendChannel::resolve: {} resolved, connect to {}",
          nodeRecord_->Address, it->host_name());
        connect(it);
      } else if (ec.value() == error::operation_aborted) {
        log->debug("SendChannel::resolve: operation aborted");
        return;
      } else {
        log->warn("SendChannel::resolve: async_resolve error: {}",
          ec.message());
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
      if (!ec) {
        log->debug("SendChannel::connect: {} connected",
          endpoint.address().to_string());
        isConnected_ = true;
        asyncSendMessage();
      } else if (ec.value() == error::operation_aborted) {
        log->debug("SendChannel::connect: operation aborted");
        return;
      } else {
        log->warn("SendChannel::connect: async_connect error: {}",
          ec.message());
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
        log->warn("SendChannel::checkIdle: channel {} timed out",
          nodeRecord_->Key);
        stop();
      } else {
        // the deadline has not passed, wait again
        checkIdle();
      }
    });
}

void SendChannel::stop()
{
  if (!stopped_) {
    log->info("SendChannel::stop: channel {} closed", nodeRecord_->Key);
    transport_.RemoveSendChannel(nodeRecord_->Key);
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
  remoteAddress_ = socket_.remote_endpoint().address().to_string();
  log->info("RecvChannel::Start: channel {}", remoteAddress_);
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
          log->error("RecvChannel::readHeader: received invalid header");
          stop();
          return;
        }
      } else if (ec.value() == error::operation_aborted) {
        log->debug("RecvChannel::readHeader: operation aborted");
        return;
      } else {
        log->error("RecvChannel::readHeader: async_read error: {}",
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
        if (header_.Method == RequestType) {
          auto msg = make_unique<raftpb::MessageBatch>();
          auto done = msg->ParseFromArray(payloadBuf_.data(), header_.Size);
          if (!done) {
            log->error("RecvChannel::readPayload: received invalid request");
            stop();
            return;
          }
          if (!transport_.HandleRequest(std::move(msg))) {
            log->error("RecvChannel::readPayload: request rejected by handler");
            stop();
            return;
          }
        } else if (header_.Method == SnapshotChunkType) {
          auto msg = make_unique<raftpb::SnapshotChunk>();
          auto done = msg->ParseFromArray(payloadBuf_.data(), header_.Size);
          if (!done) {
            log->error("RecvChannel::readPayload: received invalid chunk");
            stop();
            return;
          }
          if (!transport_.HandleSnapshotChunk(std::move(msg))) {
            log->error("RecvChannel::readPayload: chunk rejected by handler");
            stop();
            return;
          }
        } else {
          log->error("RecvChannel::readPayload: received invalid message type");
          stop();
          return;
        }
        readHeader();
      } else if (ec.value() == error::operation_aborted) {
        log->debug("RecvChannel::readPayload: operation aborted");
        return;
      } else {
        log->error("RecvChannel::readPayload: async_read error: {}",
          ec.message());
        stop();
        return;
      }
    });
}

bool RecvChannel::decodeHeader()
{
  header_ = RequestHeader::Decode(headerBuf_, RequestHeaderSize);
  if (header_.Method != RequestType && header_.Method != SnapshotChunkType) {
    log->debug("RecvChannel::decodeHeader: invalid method {}", header_.Method);
    return false;
  }
  if (header_.Size == 0) {
    log->debug("RecvChannel::decodeHeader: invalid payload size 0");
    return false;
  }
  if (header_.Size > payloadBuf_.size()) {
    payloadBuf_.resize(header_.Size);
  }
  // TODO: check crc32
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
        log->warn("RecvChannel::checkIdle: channel {} timed out",
          remoteAddress_);
        stop();
      } else {
        // the deadline has not passed, wait again
        checkIdle();
      }
    });
}

void RecvChannel::stop()
{
  if (!stopped_) {
    log->info("RecvChannel::stop: channel {} closed", remoteAddress_);
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
      throw Error(ErrorCode::UnexpectedRaftMessage, log,
        "gap found in savedChunks");
    }
    outputQueue_.push(std::move(chunk));
  }
  total_ = id;
  log->info("SnapshotLane::Start: lane {}", nodeRecord_->Key);
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
  outputQueue_.front()->set_deployment_id(transport_.GetDeploymentID());
  if (!outputQueue_.front()->witness()) {
    StatusWith<string> data = loadSnapshotChunkData(*outputQueue_.front());
    if (!data.IsOK()) {
      log->error("SnapshotLane::prepareBuffer: failed to load data from file");
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
  log->debug("SnapshotLane::sendMessage: sending chunk {}/{} to {}",
    outputQueue_.front()->chunk_id() + 1, total_, nodeRecord_->Address);
  boost::asio::async_write(socket_,
    buffer(buffer_.data(), buffer_.length()),
    [this, self = shared_from_this()](error_code ec, size_t length)
    {
      if (!ec) {
        outputQueue_.pop();
        if (!outputQueue_.empty()) {
          log->debug("SnapshotLane::sendMessage: send to {} with message: {}",
            nodeRecord_->Address, outputQueue_.front()->DebugString());
          sendMessage();
        } else {
          log->info("SnapshotLane::sendMessage: succeeded sending a snapshot "
                    "with {} chunks to {} {}",
                    total_, node_, nodeRecord_->Address);
          rejected_ = false;
          stop();
        }
      } else if (ec.value() == error::operation_aborted) {
        log->debug("SnapshotLane::sendMessage: operation aborted");
        return;
      } else {
        log->warn("SnapshotLane::sendMessage: async_write error: {}",
          ec.message());
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
        log->debug("SnapshotLane::resolve: {} resolved, connect to {}",
          nodeRecord_->Address, it->host_name());
        connect(it);
      } else if (ec.value() == error::operation_aborted) {
        log->debug("SnapshotLane::resolve: operation aborted");
        return;
      } else {
        log->warn("SnapshotLane::resolve: async_resolve error: {}",
          ec.message());
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
      if (!ec) {
        log->debug("SnapshotLane::connect: {} connected",
          endpoint.address().to_string());
        prepareBuffer();
        sendMessage();
      } else if (ec.value() == error::operation_aborted) {
        log->debug("SnapshotLane::connect: operation aborted");
        return;
      } else {
        log->warn("SnapshotLane::connect: async_connect error: {}",
          ec.message());
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
        log->warn("SnapshotLane::checkIdle: lane {} timed out",
          nodeRecord_->Key);
        stop();
      } else {
        // the deadline has not passed, wait again
        checkIdle();
      }
    });
}

void SnapshotLane::stop()
{
  if (!stopped_) {
    log->info("SnapshotLane::stop: lane {} closed", nodeRecord_->Key);
    transport_.SendSnapshotNotification(node_, rejected_);
    stopped_ = true;
    socket_.close();
    idleTimer_.cancel();
  }
}

} // namespace transport

} // namespace ycrt