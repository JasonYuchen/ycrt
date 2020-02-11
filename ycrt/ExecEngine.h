//
// Created by jason on 2020/1/16.
//

#ifndef YCRT_YCRT_EXECENGINE_H_
#define YCRT_YCRT_EXECENGINE_H_

#include <stdint.h>
#include <functional>
#include <unordered_set>
#include <vector>
#include <boost/asio.hpp>

#include "utils/Utils.h"

namespace ycrt
{

class WorkReady {
 public:
  explicit WorkReady(uint64_t count)
    : getPartition_([=](uint64_t id){ return id % count; }),
      count_(count),
      //readyClusters_(),
      readyClusterIDQueues_()
  {
    for (uint64_t i = 0; i < count; ++i) {
      //readyClusters_.emplace_back(ReadyCluster{});
      readyClusterIDQueues_.emplace_back(new BlockingConcurrentQueue<uint64_t>(1));
    }
  }
 private:
  std::function<uint64_t(uint64_t)> getPartition_;
  uint64_t count_;
  struct ReadyCluster {
    ReadyCluster()
      : mutex_(),
        map_(),
        ready_(&map_[0]),
        index_() {}
    std::mutex mutex_;
    std::unordered_set<uint64_t> map_[2];
    std::unordered_set<uint64_t> *ready_;
    uint8_t index_;
  };
  //std::vector<ReadyCluster> readyClusters_;
  std::vector<BlockingConcurrentQueueSPtr<uint64_t>> readyClusterIDQueues_;
};

class ExecEngine {
 public:
 private:
  Stopper nodeStopper_;
  Stopper taskStopper_;
  Stopper snapshotStopper_;
};

using ExecEngineSPtr = std::shared_ptr<ExecEngine>;

} // namespace ycrt



#endif //YCRT_YCRT_EXECENGINE_H_
