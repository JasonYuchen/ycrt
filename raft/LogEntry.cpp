//
// Created by jason on 2020/1/3.
//

#include "LogEntry.h"

namespace ycrt
{

namespace raft
{

using namespace std;

LogEntry::LogEntry(logdb::LogReaderSPtr logDB)
  : logDB_(std::move(logDB)),
    inMem_(logDB_->GetRange().second),
    committed_(logDB_->GetRange().first - 1),
    processed_(logDB_->GetRange().first - 1)
{}

uint64_t LogEntry::Committed() const
{
  return committed_;
}

void LogEntry::SetCommitted(uint64_t committed)
{
  committed_ = committed;
}

uint64_t LogEntry::Processed() const
{
  return processed_;
}

uint64_t LogEntry::FirstIndex() const
{
  auto index = inMem_.GetSnapshotIndex();
  if (index.IsOK()) {
    return index.GetOrThrow() + 1;
  }
  return logDB_->GetRange().first;
}

uint64_t LogEntry::LastIndex() const
{
  auto index = inMem_.GetLastIndex();
  if (index.IsOK()) {
    return index.GetOrThrow();
  }
  return logDB_->GetRange().second;
}

StatusWith<pair<uint64_t, uint64_t>> LogEntry::EntryRange() const
{
  if (inMem_.HasSnapshot() && inMem_.GetEntriesSize() == 0) {
    return ErrorCode::LogUnavailable;
  }
  return pair<uint64_t, uint64_t>{FirstIndex(), LastIndex()};
}

uint64_t LogEntry::LastTerm() const
{
  return Term(LastIndex()).GetOrThrow();
}

StatusWith<uint64_t> LogEntry::Term(uint64_t index) const
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

EntryVector LogEntry::GetUncommittedEntries() const
{
  // committed entries must have been persisted in logDB
  std::vector<pbEntrySPtr> entries;
  uint64_t low = std::max(committed_ + 1, inMem_.GetMarkerIndex());
  uint64_t high = inMem_.GetEntriesSize() + inMem_.GetMarkerIndex();
  inMem_.GetEntries(entries, low, high);
  return entries;
}

StatusWith<EntryVector> LogEntry::GetEntriesWithBound(
  uint64_t low,
  uint64_t high,
  uint64_t maxSize) const
{
  Status s = checkBound(low, high);
  if(!s.IsOK()) {
    return s;
  }
  if (low == high) {
    return EntryVector{};
  }
  uint64_t inMemoryMarker = inMem_.GetMarkerIndex();
  EntryVector entries;
  if (low >= inMemoryMarker) {
    // retrieve from inMem
    appendEntriesFromInMem(entries, low, high, maxSize);
  } else if (high <= inMemoryMarker) {
    // retrieve from logDB
    appendEntriesFromLogDB(entries, low, high, maxSize);
  } else {
    // retrieve from logDB then inMem
    Status dbs = appendEntriesFromLogDB(entries, low, inMemoryMarker, maxSize);
    if (!dbs.IsOK()) {
      return dbs;
    }
    if (entries.size() < inMemoryMarker - low) {
      // implies that the maxSize takes effect
    } else {
      uint64_t sizedb = 0;
      for (auto &ent : entries) {
        sizedb += settings::EntryNonCmdSize + ent->cmd().size();
      }
      appendEntriesFromInMem(entries, inMemoryMarker, high, maxSize - sizedb);
    }
  }
  return entries;
}

StatusWith<EntryVector> LogEntry::GetEntriesFromStart(
  uint64_t start,
  uint64_t maxSize) const
{
  if (start > LastIndex()) {
    return ErrorCode::OK;
  }
  return GetEntriesWithBound(start, LastIndex() + 1, maxSize);
}

EntryVector LogEntry::GetEntriesToApply(uint64_t limit) const
{
  if (HasEntriesToApply()) {
    return GetEntriesWithBound(
      firstNotAppliedIndex(), toApplyIndexLimit(), limit).GetOrThrow();
  }
  return {};
}

EntryVector LogEntry::GetEntriesToSave() const
{
  return inMem_.GetEntriesToSave();
}

pbSnapshotSPtr LogEntry::GetSnapshot() const
{
  if (inMem_.HasSnapshot()) {
    return inMem_.GetSnapshot();
  }
  return logDB_->GetSnapshot();
}

pbSnapshotSPtr LogEntry::GetInMemorySnapshot() const
{
  return inMem_.GetSnapshot();
}

bool LogEntry::HasEntriesToApply() const
{
  return toApplyIndexLimit() > firstNotAppliedIndex();
}

bool LogEntry::HasMoreEntriesToApply(uint64_t appliedTo) const
{
  return committed_ > appliedTo;
}

bool LogEntry::TryAppend(uint64_t index, Span<pbEntrySPtr> entries)
{
  uint64_t conflictIndex = getConflictIndex(entries);
  if (conflictIndex != 0) {
    if (conflictIndex <= committed_) {
      throw Error(ErrorCode::LogMismatch, log,
        "try append conflicts with committed entries, "
        "conflictIndex={}, committed={}", conflictIndex, committed_);
    }
    // index = m.log_index() = remote.Next-1 (see Raft::makeReplicateMessage)
    Append(entries.SubSpan(conflictIndex - index - 1));
    return true;
  }
  return false;
}

void LogEntry::Append(Span<pbEntrySPtr> entries)
{
  if (entries.empty()) {
    return;
  }
  if (entries[0]->index() <= committed_) {
    throw Error(ErrorCode::LogMismatch, log,
      "append conflicts with committed entries, "
      "first={}, committed={}", entries[0]->index(), committed_);
  }
  inMem_.Merge(entries);
}

void LogEntry::CommitTo(uint64_t index)
{
  if (index <= committed_) {
    return;
  }
  if (index > LastIndex()) {
    throw Error(ErrorCode::OutOfRange, log,
      "commit to {}, but last index={}", index, LastIndex());
  }
  committed_ = index;
}

void LogEntry::CommitUpdate(const pbUpdateCommit &uc)
{
  inMem_.CommitUpdate(uc);
  if (uc.Processed > 0) {
    if (uc.Processed < processed_ || uc.Processed > committed_) {
      throw Error(ErrorCode::OutOfRange, log,
        "invalid UpdateCommit, uc.Processed={} "
        "but processed={}, committed={}",
        uc.Processed, processed_, committed_);
    }
    processed_ = uc.Processed;
  }
  if (uc.LastApplied > 0) {
    if (uc.LastApplied > processed_ || uc.LastApplied > committed_) {
      throw Error(ErrorCode::OutOfRange, log,
        "invalid UpdateCommit, uc.LastApplied={} "
        "but processed={}, committed={}",
        uc.LastApplied, processed_, committed_);
    }
    inMem_.AppliedLogTo(uc.LastApplied);
  }
}

bool LogEntry::MatchTerm(uint64_t index, uint64_t term) const
{
  auto t = Term(index);
  if (!t.IsOK()) {
    return false;
  }
  return t.GetOrThrow() == term;
}

bool LogEntry::IsUpToDate(uint64_t index, uint64_t term) const
{
  // is remote node uptodate:
  // remote term > local term
  // or
  // remote term == local last term && remote index >= local last index
  uint64_t lastTerm = LastTerm();
  if (term >= lastTerm) {
    if (term > lastTerm) {
      return true;
    }
    return index >= LastIndex();
  }
  return false;
}

bool LogEntry::TryCommit(uint64_t index, uint64_t term)
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

void LogEntry::Restore(pbSnapshotSPtr s)
{
  committed_ = s->index();
  processed_ = s->index();
  inMem_.Restore(std::move(s));
}

void LogEntry::InMemoryResize()
{
  inMem_.Resize();
}

void LogEntry::InMemoryTryResize()
{
  inMem_.TryResize();
}

Status LogEntry::checkBound(uint64_t low, uint64_t high) const
{
  if (low > high) {
    throw Error(ErrorCode::OutOfRange, log, "low={} < high={}", low, high);
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
    throw Error(ErrorCode::OutOfRange, log, "high={} > last={}", high, last);
  }
  return ErrorCode::OK;
}

uint64_t LogEntry::firstNotAppliedIndex() const
{
  return std::max(processed_ + 1, FirstIndex());
}

uint64_t LogEntry::toApplyIndexLimit() const
{
  // uncommitted log can not be applied
  return committed_ + 1;
}

uint64_t LogEntry::getConflictIndex(Span<pbEntrySPtr> entries) const
{
  for (auto &ent : entries) {
    if (!MatchTerm(ent->index(), ent->term())) {
      return ent->index();
    }
  }
  return 0;
}

Status LogEntry::appendEntriesFromLogDB(
  EntryVector &entries,
  uint64_t low,
  uint64_t high,
  uint64_t maxSize) const
{
  return logDB_->GetEntries(entries, low, high, maxSize);
}

Status LogEntry::appendEntriesFromInMem(
  EntryVector &entries,
  uint64_t low,
  uint64_t high,
  uint64_t maxSize) const
{
  inMem_.GetEntries(entries, low, high, maxSize);
  return ErrorCode::OK;
}

} // namespace raft

} // namesapce ycrt