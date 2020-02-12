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
#include <boost/asio.hpp>
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
class SnapshotChunkFile {
 public:
  enum Mode { CREATE, READ, APPEND };
  static SnapshotChunkFile Open(boost::filesystem::path file, Mode mode);
  SnapshotChunkFile(int fd, bool syncDir, boost::filesystem::path dir);
  uint64_t Read(std::string &buf);
  uint64_t ReadAt(std::string &buf, int64_t offset);
  uint64_t Write(string_view buf);
  void Sync();
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
    Transport &transport,
    boost::asio::io_context &io,
    server::SnapshotLocator &&locator);
  // AddChunk adds a received trunk to chunks
  bool AddChunk(pbSnapshotChunkSPtr chunk);
  // RunTicker runs a timer with 1 seconds interval to trigger gc
  void RunTicker();
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
    boost::asio::io_context &io_,
    server::SnapshotLocator &&locator);
  std::shared_ptr<std::mutex> getSnapshotLock(const std::string &key);
  std::shared_ptr<track> onNewChunk(
    const std::string &key, pbSnapshotChunkSPtr chunk);
  void gc();
  void deleteTrack(const std::string &key);
  // TODO: move to the class SnapshotChunk
  void deleteTempChunkDir(const pbSnapshotChunk &chunk);
  bool shouldUpdateValidator(const pbSnapshotChunk &chunk) const;
  bool nodeRemoved(const pbSnapshotChunk &chunk) const;
  void saveChunk(const pbSnapshotChunk &chunk);
  Status finalizeSnapshot(
    const pbSnapshotChunk &chunk,
    const pbMessageBatch &msg);
  server::SnapshotEnv getSnapshotEnv(const pbSnapshotChunk &chunk) const;
  pbMessageBatchUPtr toMessageBatch(
    const pbSnapshotChunk &chunk,
    const std::vector<pbSnapshotFileSPtr> &files) const;

  const uint64_t timeoutTick_;
  const uint64_t gcTick_;
  const uint64_t maxConcurrentSlot_;

  slogger log;
  Transport &transport_;
  boost::asio::steady_timer gcTimer_;
  std::atomic_uint64_t currentTick_;
  bool validate_;
  server::SnapshotLocator snapshotLocator_;
  std::mutex mutex_;
  // guarded by mutex_, tracked_[key] is guarded by locks_[key]
  std::unordered_map<std::string, std::shared_ptr<track>> tracked_;
  // guarded by mutex_
  std::unordered_map<std::string, std::shared_ptr<std::mutex>> locks_;
};
using SnapshotChunkManagerUPtr = std::unique_ptr<SnapshotChunkManager>;

} // namespace transport

} // namespace ycrt


#endif //YCRT_TRANSPORT_SNAPSHOTCHUNKMANAGER_H_
