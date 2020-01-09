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
#include "utils/Error.h"

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
  Status Append(std::vector<pbEntry> &entries);
 private:
};
using LogDBSPtr = std::shared_ptr<LogDB>;

class LogEntry {
 public:
  explicit LogEntry(LogDBSPtr logDB)
    : logDB_(std::move(logDB)),
      inMem_(),
      committed_(),
      processed_()
  {
    auto index = logDB_->Range();
    inMem_.reset(new InMemory(index.second));
    committed_ = index.first - 1;
    processed_ = index.first - 1;
  }
  uint64_t Committed();
  void SetCommitted(uint64_t committed);
  uint64_t Processed();
  uint64_t FirstIndex();
  uint64_t LastIndex();
  std::pair<uint64_t, uint64_t> TermEntryRange();
  StatusWith<std::pair<uint64_t, uint64_t>> EntryRange();
  uint64_t LastTerm();
  StatusWith<uint64_t> Term(uint64_t index);
  Status CheckBound(uint64_t low, uint64_t high);
  std::vector<pbEntry> GetUncommittedEntries();
  StatusWith<std::vector<pbEntry>> GetEntriesFromLogDB(
    uint64_t low, uint64_t high, uint64_t maxSize);
  std::vector<pbEntry> GetEntriesFromInMem(
    uint64_t low, uint64_t high);
  StatusWith<std::vector<pbEntry>> GetEntriesWithBound(
    uint64_t low, uint64_t high);
  StatusWith<std::vector<pbEntry>> GetEntriesFromStart(
    uint64_t start, uint64_t maxSize);
  std::vector<pbEntry> GetEntriesToApply(uint64_t limit);
  std::vector<pbEntry> GetEntriesToSave();
  pbSnapshotUPtr GetSnapshot();
  uint64_t GetFirstNotAppliedIndex();
  uint64_t GetToApplyIndexLimit();
  bool HasEntriesToApply();
  bool HasMoreEntriesToApply(uint64_t appliedTo);
  bool TryAppend(uint64_t index, std::vector<pbEntry> &entries);
  void Append(std::vector<pbEntry> &entries);
  uint64_t GetConflictIndex(std::vector<pbEntry> &entries);
  void CommitTo(uint64_t index);
  void CommitUpdate(/*pb.UpdateCommit*/);
  bool MatchTerm(uint64_t index, uint64_t term);
  bool IsUpToDate(uint64_t index, uint64_t term);
  bool TryCommit(uint64_t index, uint64_t term);
  void Restore(pbSnapshotSPtr);
  void InMemoryGC();
 private:
  LogDBSPtr logDB_;
  InMemorySPtr inMem_;
  uint64_t committed_;
  uint64_t processed_;
};
using LogEntrySPtr = std::shared_ptr<LogEntry>;

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGENTRY_H_
