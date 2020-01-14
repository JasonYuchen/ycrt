//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_RAFT_LOGENTRY_H_
#define YCRT_RAFT_LOGENTRY_H_

#include <utility>
#include <memory>
#include <stdint.h>
#include "pb/RaftMessage.h"
#include "InMemory.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace raft
{

class LogDB {
 public:
  std::pair<uint64_t, uint64_t> Range();
  void SetRange(uint64_t index, uint64_t length);
  std::pair<pbStateSPtr, pbMembershipSPtr> NodeState();
  void SetNodeState(pbStateSPtr);
  Status CreateSnapshot(pbSnapshotUPtr);
  Status ApplySnapshot(pbSnapshotUPtr);
  StatusWith<uint64_t> Term(uint64_t index);
  StatusWith<std::vector<pbEntry>> Entries(
    uint64_t low, uint64_t high, uint64_t maxSize);
  pbSnapshotUPtr Snapshot();
  Status Compact(uint64_t index);
  Status Append(Span<pbEntry> entries);
 private:
};
using LogDBUPtr = std::unique_ptr<LogDB>;

class LogEntry {
 public:
  explicit LogEntry(LogDBUPtr logDB)
    : logDB_(std::move(logDB)),
      inMem_(new InMemory(logDB_->Range().second)),
      committed_(logDB_->Range().first - 1),
      processed_(logDB_->Range().first - 1)
  {
  }
  uint64_t Committed()
  {
    return 0;
  }
  void SetCommitted(uint64_t committed)
  {

  }
  uint64_t Processed()
  {
    return 0;
  }
  uint64_t FirstIndex()
  {
    return 0;
  }
  uint64_t LastIndex()
  {
    return 0;
  }
  std::pair<uint64_t, uint64_t> TermEntryRange()
  {
    return std::pair<uint64_t, uint64_t>();
  }
  StatusWith<std::pair<uint64_t, uint64_t>> EntryRange()
  {
    return StatusWith<std::pair<uint64_t, uint64_t>>(ErrorCode::OutOfRange);
  }
  uint64_t LastTerm()
  {
    return 0;
  }
  StatusWith<uint64_t> Term(uint64_t index)
  {
    return StatusWith<uint64_t>(ErrorCode::OutOfRange);
  }
  Status CheckBound(uint64_t low, uint64_t high)
  {
    return Status();
  }
  std::vector<pbEntry> GetUncommittedEntries()
  {
    return std::vector<pbEntry>();
  }
  StatusWith<std::vector<pbEntry>> GetEntriesFromLogDB(
    uint64_t low, uint64_t high, uint64_t maxSize)
  {
    return StatusWith<std::vector<pbEntry>>(ErrorCode::OutOfRange);
  }
  std::vector<pbEntry> GetEntriesFromInMem(
    uint64_t low, uint64_t high)
  {
    return std::vector<pbEntry>();
  }
  StatusWith<std::vector<pbEntry>> GetEntriesWithBound(
    uint64_t low, uint64_t high)
  {
    return StatusWith<std::vector<pbEntry>>(ErrorCode::OutOfRange);
  }
  StatusWith<std::vector<pbEntry>> GetEntriesFromStart(
    uint64_t start, uint64_t maxSize)
  {
    return StatusWith<std::vector<pbEntry>>(ErrorCode::OutOfRange);
  }
  std::vector<pbEntry> GetEntriesToApply(uint64_t limit)
  {
    return std::vector<pbEntry>();
  }
  std::vector<pbEntry> GetEntriesToSave()
  {
    return std::vector<pbEntry>();
  }
  pbSnapshotUPtr GetSnapshot()
  {
    return ycrt::pbSnapshotUPtr();
  }
  uint64_t GetFirstNotAppliedIndex()
  {
    return 0;
  }
  uint64_t GetToApplyIndexLimit()
  {
    return 0;
  }
  bool HasEntriesToApply()
  {
    return false;
  }
  bool HasMoreEntriesToApply(uint64_t appliedTo)
  {
    return false;
  }
  bool TryAppend(uint64_t index, Span<pbEntry> entries)
  {
    return false;
  }
  void Append(Span<pbEntry> entries)
  {

  }
  uint64_t GetConflictIndex(Span<pbEntry> entries)
  {
    return 0;
  }
  void CommitTo(uint64_t index)
  {

  }
  void CommitUpdate(/*pb.UpdateCommit*/)
  {

  }
  bool MatchTerm(uint64_t index, uint64_t term)
  {
    return false;
  }
  bool IsUpToDate(uint64_t index, uint64_t term)
  {
    return false;
  }
  bool TryCommit(uint64_t index, uint64_t term)
  {
    return false;
  }
  void Restore(const pbSnapshot &s)
  {

  }
  void InMemoryGC()
  {

  }
 private:
  LogDBUPtr logDB_;
  InMemoryUPtr inMem_;
  uint64_t committed_;
  uint64_t processed_;
};
using LogEntryUPtr = std::unique_ptr<LogEntry>;

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGENTRY_H_
