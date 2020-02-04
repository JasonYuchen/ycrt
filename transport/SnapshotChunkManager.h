//
// Created by jason on 2019/12/31.
//

#ifndef YCRT_TRANSPORT_SNAPSHOTCHUNKMANAGER_H_
#define YCRT_TRANSPORT_SNAPSHOTCHUNKMANAGER_H_

#include <stdint.h>
#include <string>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace transport
{

class Transport;
class SnapshotChunkManager {
 public:
  std::unique_ptr<SnapshotChunkManager> New(
    Transport *transport_,
    //std::function<void(pbMessageBatchUPtr)> &&onReceive, // Transport::handleRequest
    //std::function<void(uint64_t, uint64_t, uint64_t)> &&confirm, // Transport::handleSnapshotConfirm
    //std::function<uint64_t()> &&deploymentIDFunc, // Transport::deploymentID_
    std::function<std::string(uint64_t, uint64_t)> &&getSnapshotDir);
 private:
  SnapshotChunkManager();

  slogger log;
  Transport *transport_;
  uint64_t currentTick_;
  bool validate_;
  std::function<std::string(uint64_t, uint64_t)> getSnapshotDir_;
  struct track {
    pbSnapshotChunkSPtr firstChunk;
    std::vector<pbSnapshotFileSPtr> extraFiles;
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


#endif //YCRT_TRANSPORT_SNAPSHOTCHUNKMANAGER_H_
