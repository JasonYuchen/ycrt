//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_RAFT_LOGENTRY_H_
#define YCRT_RAFT_LOGENTRY_H_

#include <utility>
#include <memory>
#include <stdint.h>
#include "pb/RaftMessage.h"
#include "logdb/InMemory.h"
#include "logdb/LogReader.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace raft
{

class LogEntry {
 public:
  explicit LogEntry(logdb::LogReaderSPtr logDB);
  DISALLOW_COPY_AND_ASSIGN(LogEntry);
  uint64_t Committed() const;
  void SetCommitted(uint64_t committed);
  uint64_t Processed() const;
  uint64_t FirstIndex() const;
  uint64_t LastIndex() const;
  StatusWith<std::pair<uint64_t, uint64_t>> EntryRange() const;
  uint64_t LastTerm() const;
  StatusWith<uint64_t> Term(uint64_t index) const;
  EntryVector GetUncommittedEntries() const;
  StatusWith<EntryVector> GetEntriesWithBound(
    uint64_t low, uint64_t high, uint64_t maxSize) const;
  StatusWith<EntryVector> GetEntriesFromStart(
    uint64_t start, uint64_t maxSize) const;
  EntryVector GetEntriesToApply(
    uint64_t limit = settings::Soft::ins().MaxEntrySize) const;
  EntryVector GetEntriesToSave() const;
  pbSnapshotSPtr GetSnapshot() const;
  pbSnapshotSPtr GetInMemorySnapshot() const;
  bool HasEntriesToApply() const;
  bool HasMoreEntriesToApply(uint64_t appliedTo) const;
  bool TryAppend(uint64_t index, Span<pbEntrySPtr> entries);
  void Append(Span<pbEntrySPtr> entries);
  void CommitTo(uint64_t index);
  void CommitUpdate(const pbUpdateCommit &uc);
  bool MatchTerm(uint64_t index, uint64_t term) const;
  bool IsUpToDate(uint64_t index, uint64_t term) const;
  bool TryCommit(uint64_t index, uint64_t term);
  void Restore(pbSnapshotSPtr s);
  void InMemoryResize();
  void InMemoryTryResize();
 private:
  Status checkBound(uint64_t low, uint64_t high) const;
  uint64_t firstNotAppliedIndex() const;
  uint64_t toApplyIndexLimit() const;
  uint64_t getConflictIndex(Span<pbEntrySPtr> entries) const;
  Status appendEntriesFromLogDB(EntryVector &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const;
  Status appendEntriesFromInMem(EntryVector &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const;

  slogger log;
  logdb::LogReaderSPtr logDB_;
  logdb::InMemory inMem_;
  uint64_t committed_;
  uint64_t processed_;
};
using LogEntryUPtr = std::unique_ptr<LogEntry>;

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGENTRY_H_
