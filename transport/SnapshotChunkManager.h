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
    Transport &transport_,
    //std::function<void(pbMessageBatchUPtr)> &&onReceive, // Transport::handleRequest
    //std::function<void(uint64_t, uint64_t, uint64_t)> &&confirm, // Transport::handleSnapshotConfirm
    //std::function<uint64_t()> &&deploymentIDFunc, // Transport::deploymentID_
    std::function<std::string(uint64_t, uint64_t)> &&getSnapshotDir);

  // AddChunk adds a received trunk to chunks
  bool AddChunk(pbSnapshotChunkSPtr chunk);
  void Tick();
 private:
  SnapshotChunkManager(
    Transport &transport_,
    std::function<std::string(uint64_t, uint64_t)> &&getSnapshotDir);
  std::shared_ptr<std::mutex> getSnapshotLock(const std::string &key);
  bool onNewChunk(const std::string &key, pbSnapshotChunkSPtr chunk);

  void gc();
  void deleteTempChunkDir(const pbSnapshotChunk &chunk);
  bool shouldUpdateValidator(const pbSnapshotChunk &chunk);


  const uint64_t timeoutTick_;
  const uint64_t gcTick_;
  const uint64_t maxConcurrentSlot_;

  slogger log;
  Transport &transport_;
  std::atomic_uint64_t currentTick_;
  bool validate_;
  std::function<std::string(uint64_t, uint64_t)> getSnapshotDir_;
  struct track {
    pbSnapshotChunkSPtr firstChunk;
    std::vector<pbSnapshotFileSPtr> extraFiles;
    // validator
    uint64_t nextChunk;
    uint64_t tick;
  };
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<track>> tracked_; // guarded by mutex_
  std::unordered_map<std::string, std::shared_ptr<std::mutex>> locks_; // guarded by mutex_
};

} // namespace transport

} // namespace ycrt


#endif //YCRT_TRANSPORT_SNAPSHOTCHUNKMANAGER_H_
