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
#include "server/SnapshotEnv.h"

namespace ycrt
{

namespace transport
{

// {snapshot} - split, send (managed by Transport) ->
// {chunk1 chunk2 ...} - receive, merge (managed by SnapshotChunkFile) ->
// {snapshot}
// TODO: consider throw instead of Status
class SnapshotChunkFile {
 public:
  enum Mode { CREATE, READ, APPEND };
  static SnapshotChunkFile Open(boost::filesystem::path file, Mode mode);
  SnapshotChunkFile(int fd, bool syncDir, boost::filesystem::path dir);
  StatusWith<uint64_t> Read(std::string &buf);
  StatusWith<uint64_t> ReadAt(std::string &buf, int64_t offset);
  StatusWith<uint64_t> Write(const std::string &buf);
  Status Sync();
  ~SnapshotChunkFile();
 private:
  int fd_;
  bool syncDir_;
  boost::filesystem::path dir_;
};

class Transport;
class SnapshotChunkManager {
 public:
  static std::unique_ptr<SnapshotChunkManager> New(
    Transport &transport_,
    //std::function<void(pbMessageBatchUPtr)> &&onReceive, // Transport::handleRequest
    //std::function<void(uint64_t, uint64_t, uint64_t)> &&confirm, // Transport::handleSnapshotConfirm
    //std::function<uint64_t()> &&deploymentIDFunc, // Transport::deploymentID_
    server::SnapshotLocator &&locator);

  // AddChunk adds a received trunk to chunks
  bool AddChunk(pbSnapshotChunkSPtr chunk);
  void Tick();
 private:
  struct track {
    pbSnapshotChunkSPtr firstChunk;
    std::vector<pbSnapshotFileSPtr> extraFiles;
    // validator
    uint64_t nextChunk;
    uint64_t tick;
  };

  SnapshotChunkManager(
    Transport &transport_,
    std::function<std::string(uint64_t, uint64_t)> &&getSnapshotDir);
  std::shared_ptr<std::mutex> getSnapshotLock(const std::string &key);
  std::shared_ptr<track> onNewChunk(const std::string &key, pbSnapshotChunkSPtr chunk);
  void gc();
  void deleteTrack(const std::string &key);
  // TODO: move to the class SnapshotChunk
  void deleteTempChunkDir(const pbSnapshotChunk &chunk);
  bool shouldUpdateValidator(const pbSnapshotChunk &chunk);
  StatusWith<bool> nodeRemoved(const pbSnapshotChunk &chunk);
  Status saveChunk(const pbSnapshotChunk &chunk);
  Status finalizeSnapshot(const pbSnapshotChunk &chunk, const pbMessageBatch &msg);
  server::SnapshotEnv getSnapshotEnv(const pbSnapshotChunk &chunk);
  pbMessageBatchUPtr toMessageBatch(const pbSnapshotChunk &chunk, const std::vector<pbSnapshotFileSPtr> &files);
  const uint64_t timeoutTick_;
  const uint64_t gcTick_;
  const uint64_t maxConcurrentSlot_;

  slogger log;
  Transport &transport_;
  std::atomic_uint64_t currentTick_;
  bool validate_;
  std::function<std::string(uint64_t, uint64_t)> snapshotLocator_;
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<track>> tracked_; // guarded by mutex_, tracked_[key] is guarded by locks_[key]
  std::unordered_map<std::string, std::shared_ptr<std::mutex>> locks_; // guarded by mutex_
};
using SnapshotChunkManagerUPtr = std::unique_ptr<SnapshotChunkManager>;

} // namespace transport

} // namespace ycrt


#endif //YCRT_TRANSPORT_SNAPSHOTCHUNKMANAGER_H_
