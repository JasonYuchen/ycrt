//
// Created by jason on 2019/12/31.
//

#ifndef YCRT_TRANSPORT_SNAPSHOTCHUNK_H_
#define YCRT_TRANSPORT_SNAPSHOTCHUNK_H_

#include <stdint.h>
#include <string>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace transport
{

class SnapshotChunk {
 public:
  std::unique_ptr<SnapshotChunk> New(
    std::function<void(MessageBatchUPtr)> onReceive,
    std::function<void(uint64_t, uint64_t, uint64_t)> confirm,
    std::function<uint64_t()> deploymentIDFunc,
    std::function<std::string(uint64_t, uint64_t)> snapshotDirFunc);
 private:
  SnapshotChunk();
  uint64_t currentTick_;
  bool validate_;
  std::function<std::string(uint64_t, uint64_t)> getSnapshotDir_;
  std::function<void(MessageBatchUPtr)> onReceive_;
  std::function<void(uint64_t, uint64_t, uint64_t)> confirm_;
  std::function<uint64_t()> getDeploymentID_;
  struct track {
    SnapshotChunkUPtr firstChunk;
    std::vector<SnapshotFileUPtr> extraFiles;
    // validator
    uint64_t nextChunk;
    uint64_t tick;
  };
  std::unordered_map<std::string, track> tracked_;
  std::unordered_map<std::string, std::mutex> locks_;
  uint64_t timeoutTick_;
  uint64_t gcTick_;
  uint64_t maxConcurrentSlot_;
  std::mutex mutex_;
};

} // namespace transport

} // namespace ycrt


#endif //YCRT_TRANSPORT_SNAPSHOTCHUNK_H_
