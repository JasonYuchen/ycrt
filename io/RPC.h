//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_IO_RPC_H_
#define YCRT_IO_RPC_H_

#include <string>
#include <memory>
#include "pb/raft.pb.h"

namespace ycrt
{

namespace io
{

class Connection
  : public std::enable_shared_from_this<Connection> {
 public:
  void close();
  void sendMessageBatch(std::unique_ptr<raftpb::MessageBatch> batch);
 private:
};

using ConnectionPtr = std::shared_ptr<Connection>;

class SnapshotConnection
  : public std::enable_shared_from_this<SnapshotConnection> {
 public:
  void close();
  void sendSnapshotChunk(std::unique_ptr<raftpb::SnapshotChunk> chunk);
 private:
};

using SnapshotConnectionPtr = std::shared_ptr<SnapshotConnection>;

class RaftRPC {
 public:
  RaftRPC();
  std::string name();
  void start();
  void stop();
  ConnectionPtr getConnection(/*TODO*/std::string target);
  SnapshotConnectionPtr getSnapshotConnection(/*TODO*/std::string target);
 private:
  std::atomic_bool stop_;

};

} // namespace io

} // namespace ycrt

#endif //YCRT_IO_RPC_H_
