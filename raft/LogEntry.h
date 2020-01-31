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
#include "LogEntryUtils.h"

namespace ycrt
{

namespace raft
{

// ILogDB is a read-only interface to the underlying persistent storage to
// allow the raft package to access raft state, entries, snapshots stored in
// the persistent storage. Entries stored in the persistent storage accessible
// via ILogDB is usually not required in normal cases.
class LogDB {
 public:
  std::pair<uint64_t, uint64_t> Range() const;
  void SetRange(uint64_t index, uint64_t length);
  std::pair<pbStateSPtr, pbMembershipSPtr> NodeState() const;
  void SetNodeState(pbStateSPtr);
  Status CreateSnapshot(pbSnapshotUPtr);
  Status ApplySnapshot(pbSnapshotUPtr);
  StatusWith<uint64_t> Term(uint64_t index) const;
  Status GetEntries(std::vector<pbEntry> &entries,
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
      inMem_(logDB_->Range().second),
      committed_(logDB_->Range().first - 1),
      processed_(logDB_->Range().first - 1)
  {
  }
  uint64_t Committed() const
  {
    return committed_;
  }
  void SetCommitted(uint64_t committed)
  {
    committed_ = committed;
  }

  uint64_t Processed() const
  {
    return processed_;
  }

  uint64_t FirstIndex() const
  {
    auto index = inMem_.GetSnapshotIndex();
    if (index.IsOK()) {
      return index.GetOrThrow() + 1;
    }
    return logDB_->Range().first;
  }

  uint64_t LastIndex() const
  {
    auto index = inMem_.GetLastIndex();
    if (index.IsOK()) {
      return index.GetOrThrow();
    }
    return logDB_->Range().second;
  }

  StatusWith<std::pair<uint64_t, uint64_t>> EntryRange() const
  {
    if (inMem_.HasSnapshot() && inMem_.GetEntriesSize() == 0) {
      return ErrorCode::LogUnavailable;
    }
    return std::pair<uint64_t, uint64_t>{FirstIndex(), LastIndex()};
  }

  uint64_t LastTerm() const
  {
    return Term(LastIndex()).GetOrThrow();
  }

  StatusWith<uint64_t> Term(uint64_t index) const
  {
    // TermEntryRange
    // for firstIndex(), when it is determined by the inmem, what we actually
    // want to return is the snapshot index, l.firstIndex() - 1 is thus required
    // when it is determined by the logdb component, other than actual entries
    // we have a marker entry with known index/term (but not type or data),
    // use l.firstIndex()-1 to include this marker element.
    // as we don't have the type/data of the marker entry, it is only used in
    // term(), we can not pull its value and send it to the RSM for execution.
    uint64_t first = FirstIndex() - 1;
    uint64_t last = LastIndex();
    if (index < first || index > last) {
      return 0; // OK
    }
    auto term = inMem_.GetTerm(index);
    if (term.IsOK()) {
      return term;
    }
    term = logDB_->Term(index);
    if (!term.IsOK() &&
      term.Code() != ErrorCode::LogUnavailable &&
      term.Code() != ErrorCode::LogCompacted) {
      term.IsOKOrThrow();
    }
    return term;
  }

  Status CheckBound(uint64_t low, uint64_t high) const
  {
    if (low > high) {
      throw Error(ErrorCode::OutOfRange, "low={0} < high={1}", low, high);
    }
    auto range = EntryRange();
    if (!range.IsOK()) {
      return ErrorCode::LogCompacted;
    }
    uint64_t first = range.GetOrThrow().first;
    uint64_t last = range.GetOrThrow().second;
    if (low < first) {
      return ErrorCode::LogCompacted;
    }
    if (high > last + 1) {
      throw Error(ErrorCode::OutOfRange, "high={0} > last={1}", high, last);
    }
    return ErrorCode::OK;
  }

  std::vector<pbEntry> GetUncommittedEntries() const
  {
    // committed entries must have been persisted in logDB
    std::vector<pbEntry> entries;
    uint64_t low = std::max(committed_ + 1, inMem_.GetMarkerIndex());
    uint64_t high = inMem_.GetEntriesSize() + inMem_.GetMarkerIndex();
    inMem_.GetEntries(entries, low, high);
    return entries;
  }

  StatusWith<std::vector<pbEntry>> GetEntriesWithBound(
    uint64_t low, uint64_t high, uint64_t maxSize) const
  {
    Status s = CheckBound(low, high);
    if(!s.IsOK()) {
      return s;
    }
    if (low == high) {
      return std::vector<pbEntry>{};
    }
    uint64_t inMemoryMarker = inMem_.GetMarkerIndex();
    std::vector<pbEntry> entries;
    if (low >= inMemoryMarker) {
      // retrieve from inMem
      appendEntriesFromInMem(entries, low, high, maxSize);
    } else if (high <= inMemoryMarker) {
      // retrieve from logDB
      appendEntriesFromLogDB(entries, low, high, maxSize);
    } else {
      // retrieve from logDB then inMem
      auto entsdb = appendEntriesFromLogDB(entries, low, inMemoryMarker, maxSize);
      if (!entsdb.IsOK()) {
        return entsdb;
      }
      if (entries.size() < inMemoryMarker - low) {
        // implies that the maxSize takes effect
      } else {
        uint64_t sizedb = 0;
        for (auto &ent : entries) {
          sizedb += settings::EntryNonCmdSize + ent.cmd().size();
        }
        appendEntriesFromInMem(entries, inMemoryMarker, high, maxSize - sizedb);
      }
    }
    return entries;
  }

  StatusWith<std::vector<pbEntry>> GetEntriesFromStart(
    uint64_t start, uint64_t maxSize) const
  {
    if (start > LastIndex()) {
      return ErrorCode::OK;
    }
    return GetEntriesWithBound(start, LastIndex() + 1, maxSize);
  }

  std::vector<pbEntry> GetEntriesToApply(
    uint64_t limit = settings::Soft::ins().MaxEntrySize) const
  {
    return std::vector<pbEntry>();
  }

  std::vector<pbEntry> GetEntriesToSave() const
  {
    return std::vector<pbEntry>();
  }

  pbSnapshotUPtr GetSnapshot() const
  {
    return ycrt::pbSnapshotUPtr();
  }

  uint64_t GetFirstNotAppliedIndex() const
  {
    return 0;
  }

  uint64_t GetToApplyIndexLimit() const
  {
    return 0;
  }

  bool HasEntriesToApply() const
  {
    return false;
  }

  bool HasMoreEntriesToApply(uint64_t appliedTo) const
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

  uint64_t GetConflictIndex(Span<pbEntry> entries) const
  {
    return 0;
  }

  void CommitTo(uint64_t index)
  {

  }

  void CommitUpdate(/*pb.UpdateCommit*/)
  {

  }

  bool MatchTerm(uint64_t index, uint64_t term) const
  {
    return false;
  }

  bool IsUpToDate(uint64_t index, uint64_t term) const
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
  Status appendEntriesFromLogDB(std::vector<pbEntry> &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const
  {
    return logDB_->GetEntries(entries, low, high, maxSize);
  }

  Status appendEntriesFromInMem(std::vector<pbEntry> &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const
  {
    inMem_.GetEntries(entries, low, high, maxSize);
    return ErrorCode::OK;
  }

  slogger log;
  LogDBUPtr logDB_;
  InMemory inMem_;
  uint64_t committed_;
  uint64_t processed_;
};
using LogEntryUPtr = std::unique_ptr<LogEntry>;

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGENTRY_H_
