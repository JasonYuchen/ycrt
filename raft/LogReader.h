//
// Created by jason on 2020/2/1.
//

#ifndef YCRT_RAFT_LOGREADER_H_
#define YCRT_RAFT_LOGREADER_H_

#include <stdint.h>
#include <vector>
#include <memory>
#include <mutex>
#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace raft
{

// actual persistent log manager (backed up by RocksDB)
class LogDBContext {
 public:
  void Destroy();
  void Reset();
  // TODO
};

class LogDB {
 public:
  const std::string &Name() const noexcept;
  void Close() noexcept;
  uint32_t BinaryFormat() const noexcept;
  // TODO
};
using LogDBSPtr = std::shared_ptr<LogDB>;

class LogReader {
 public:
  static std::unique_ptr<LogReader> New(uint64_t clusterID, uint64_t nodeID, LogDBSPtr logdb)
  {
    std::unique_ptr<LogReader> reader(new LogReader(clusterID, nodeID, std::move(logdb)));
    return reader;
  }
  std::pair<uint64_t, uint64_t> Range() const;
  void SetRange(uint64_t index, uint64_t length);

  std::pair<pbStateSPtr, pbMembershipSPtr> NodeState()
  {
    std::unique_lock<std::mutex> guard(mutex_);
    return {state_, std::make_shared<pbMembership>(snapshot_->membership())}; // FIXME
  }

  void SetNodeState(const pbState &state);
  Status CreateSnapshot(pbSnapshotUPtr);
  Status ApplySnapshot(pbSnapshotUPtr);
  StatusWith<uint64_t> Term(uint64_t index) const;

  Status GetEntries(std::vector<pbEntry> &entries,
    uint64_t low, uint64_t high, uint64_t maxSize)
  {
    // TODO
  }

  pbSnapshotSPtr Snapshot();
  Status Compact(uint64_t index);
  Status Append(Span<pbEntry> entries);
 private:
  LogReader(uint64_t clusterID, uint64_t nodeID, LogDBSPtr logdb)
    : mutex_(),
      clusterID_(clusterID),
      nodeID_(nodeID),
      cn_(FmtClusterNode(clusterID, nodeID)),
      logdb_(std::move(logdb)),
      state_(),
      snapshot_(),
      markerIndex_(0),
      markerTerm_(0),
      length_(1)
  {}

  std::string describe() const noexcept
  {
    return fmt::format(
      "LogReader[markerIndex={0},markerTerm={1},length={2}] {3}",
      markerIndex_, markerTerm_, length_, cn_);
  }

  std::mutex mutex_;
  uint64_t clusterID_;
  uint64_t nodeID_;
  const std::string cn_;
  LogDBSPtr logdb_;
  pbStateSPtr state_;
  pbSnapshotSPtr snapshot_;
  uint64_t markerIndex_; // different from that of InMemory::markerIndex
  uint64_t markerTerm_;
  uint64_t length_;

};
using LogReaderSPtr = std::shared_ptr<LogReader>;

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGREADER_H_
