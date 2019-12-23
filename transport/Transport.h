//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_TRANSPORT_TRANSPORT_H_
#define YCRT_TRANSPORT_TRANSPORT_H_

#include <boost/lockfree/queue.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <pb/raft.pb.h>

namespace ycrt
{

namespace transport
{

class SendQueue {
 public:
  SendQueue(uint64_t sendQueueLength) : queue_(sendQueueLength) {
    queue_.pop()
  }
 private:
  boost::lockfree::queue<std::shared_ptr<raftpb::Message>> queue_;
};

class Transport {
 public:
  Transport();
  std::string name();
  //void setUnmanagedDeploymentID();
  void setDeploymentID(uint64_t id);

  // TODO: maybe just remove these interfaces, handle the message by Transport?
  void setMessageHandler(RaftMessageHandler handler);
  void removeMessageHandler();
  bool asyncSendMessage(std::unique_ptr<raftpb::Message> m);
  bool asyncSendSnapshot(std::unique_ptr<raftpb::Message> m);
  //std::shared_ptr<Sink> getStreamConnection(uint64_t clusterID, uint64_t nodeID);
  void stop();
 private:
  std::shared_ptr<spdlog::logger> log;
  uint64_t streamConnections;
  uint64_t sendQueueLength;
  uint64_t getConnectedTimeoutS;
  uint64_t idleTimeoutS;
  std::runtime_error errChunkSendSkipped;
  std::runtime_error errBatchSendSkipped;

  uint64_t deploymentID;
  std::mutex mutex_;
  std::unordered_map<std::string, SendQueue> queues_;
};

} // namespace transport

} // namespace ycrt

#endif //YCRT_TRANSPORT_TRANSPORT_H_
