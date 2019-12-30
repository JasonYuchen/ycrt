//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_IO_RPC_H_
#define YCRT_IO_RPC_H_

#include <string>
#include <memory>
#include <boost/asio.hpp>
#include "pb/raft.pb.h"
#include "ycrt/Config.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace io
{

class ConnectionImpl;
class Connection {
 public:
  void close();
  void sendMessageBatch(std::unique_ptr<raftpb::MessageBatch> batch);
 private:
  std::unique_ptr<ConnectionImpl> impl_;
};

using ConnectionPtr = std::shared_ptr<Connection>;

class SnapshotConnection {
 public:
  void close();
  void sendSnapshotChunk(std::unique_ptr<raftpb::SnapshotChunk> chunk);
 private:
  std::unique_ptr<ConnectionImpl> impl_;
};

using SnapshotConnectionPtr = std::shared_ptr<SnapshotConnection>;

class RPC {
 public:
  RPC();
  std::string name();
  void start();
  void stop();
  ConnectionPtr getConnection(/*TODO*/std::string target);
  SnapshotConnectionPtr getSnapshotConnection(/*TODO*/std::string target);
 private:
  friend class ConnectionImpl;
  void serveConnection(std::shared_ptr<ConnectionImpl> &conn);

  slogger log;
  std::atomic_bool stop_;
  NodeHostConfig nhConifg_;
  std::function<void(std::unique_ptr<raftpb::MessageBatch>)> requestHandler_;
  std::function<void(std::unique_ptr<raftpb::SnapshotChunk>)> chunkHandler_;
  boost::asio::io_context io_context_;
  boost::asio::ip::tcp::acceptor acceptor_;
};

} // namespace io

} // namespace ycrt

#endif //YCRT_IO_RPC_H_
