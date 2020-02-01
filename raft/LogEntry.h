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
  pbSnapshotSPtr Snapshot();
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
    if (HasEntriesToApply()) {
      return GetEntriesWithBound(
        firstNotAppliedIndex(), toApplyIndexLimit(), limit).GetOrThrow();
    }
    return {};
  }

  std::vector<pbEntry> GetEntriesToSave() const
  {
    return inMem_.GetEntriesToSave();
  }

  pbSnapshotSPtr GetSnapshot() const
  {
    if (inMem_.HasSnapshot()) {
      return inMem_.GetSnapshot();
    }
    return logDB_->Snapshot();
  }

  bool HasEntriesToApply() const
  {
    return toApplyIndexLimit() > firstNotAppliedIndex();
  }

  bool HasMoreEntriesToApply(uint64_t appliedTo) const
  {
    return committed_ > appliedTo;
  }

  bool TryAppend(uint64_t index, const Span<pbEntry> entries)
  {
    uint64_t conflictIndex = getConflictIndex(entries);
    if (conflictIndex != 0) {
      if (conflictIndex <= committed_) {
        throw Error(ErrorCode::LogMismatch, log,
          "try append conflicts with committed entries, "
          "conflictIndex={0}, committed={1}", conflictIndex, committed_);
      }
      // index = m.log_index() = remote.Next-1 (see Raft::makeReplicateMessage)
      Append(entries.SubSpan(conflictIndex - index - 1));
    }
    return false;
  }

  void Append(const Span<pbEntry> entries)
  {
    if (entries.empty()) {
      return;
    }
    if (entries[0].index() <= committed_) {
      throw Error(ErrorCode::LogMismatch, log,
        "append conflicts with committed entries, "
        "first={0}, committed={1}", entries[0].index(), committed_);
    }
    inMem_.Merge(entries);
  }

  void CommitTo(uint64_t index)
  {
    if (index <= committed_) {
      return;
    }
    if (index > LastIndex()) {
      throw Error(ErrorCode::OutOfRange, log,
        "commit to {0}, but last index={1}", index, LastIndex());
    }
    committed_ = index;
  }

  void CommitUpdate(const pbUpdateCommit &uc)
  {
    inMem_.CommitUpdate(uc);
    if (uc.Processed > 0) {
      if (uc.Processed < processed_ || uc.Processed > committed_) {
        throw Error(ErrorCode::OutOfRange, log,
          "invalid UpdateCommit, uc.Processed={0} "
          "but processed={1}, committed={2}",
          uc.Processed, processed_, committed_);
      }
      processed_ = uc.Processed;
    }
    if (uc.LastApplied > 0) {
      if (uc.LastApplied > processed_ || uc.LastApplied > committed_) {
        throw Error(ErrorCode::OutOfRange, log,
          "invalid UpdateCommit, uc.LastApplied={0} "
          "but processed={1}, committed={2}",
          uc.LastApplied, processed_, committed_);
      }
      inMem_.AppliedLogTo(uc.LastApplied);
    }
  }

  bool MatchTerm(uint64_t index, uint64_t term) const
  {
    auto t = Term(index);
    if (!t.IsOK()) {
      return false;
    }
    return t.GetOrThrow() == term;
  }

  // is remote node uptodate:
  // remote term > local term
  // or
  // remote term == local last term && remote index >= local last index
  bool IsUpToDate(uint64_t index, uint64_t term) const
  {
    uint64_t lastTerm = LastTerm();
    if (term >= lastTerm) {
      if (term > lastTerm) {
        return true;
      }
      return index >= LastIndex();
    }
    return false;
  }

  bool TryCommit(uint64_t index, uint64_t term)
  {
    if (index <= committed_) {
      return false;
    }
    auto _term = Term(index);
    if (!_term.IsOK() && _term.Code() != ErrorCode::LogCompacted) {
      _term.IsOKOrThrow();
    }
    if (index > committed_ && term == _term.GetOrDefault(0)) {
      CommitTo(index);
      return true;
    }
    return false;
  }

  // FIXME: change arg to pbSnapshotSPtr
  void Restore(const pbSnapshot &s)
  {
    inMem_.Restore(std::make_shared<pbSnapshot>(s));
    committed_ = s.index();
    processed_ = s.index();
  }

  void InMemoryResize()
  {
    inMem_.Resize();
  }

  void InMemoryTryResize()
  {
    inMem_.TryResize();
  }

 private:
  uint64_t firstNotAppliedIndex() const
  {
    return std::max(processed_ + 1, FirstIndex());
  }

  uint64_t toApplyIndexLimit() const
  {
    // uncommitted log can not be applied
    return committed_ + 1;
  }

  uint64_t getConflictIndex(const Span<pbEntry> entries) const
  {
    for (const auto &ent : entries) {
      if (!MatchTerm(ent.index(), ent.term())) {
        return ent.index();
      }
    }
    return 0;
  }

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
