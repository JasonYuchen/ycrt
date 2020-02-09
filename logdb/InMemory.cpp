//
// Created by jason on 2020/1/3.
//

#include "InMemory.h"

namespace ycrt
{

namespace logdb
{

void CheckEntriesToAppend(Span<pbEntrySPtr> existing, Span<pbEntrySPtr> append)
{
  if (existing.empty() || append.empty()) {
    return;
  }
  if (existing.back()->index() + 1 != append.front()->index()) {
    throw Error(ErrorCode::LogMismatch,
      "found a hold, exist {}, append {}",
      existing.back()->index(), append.front()->index());
  }
  if (existing.back()->term() > append.front()->term()) {
    throw Error(ErrorCode::LogMismatch,
      "unexpected term, ecist {}, append {}",
      existing.back()->term(), append.front()->term());
  }
}

InMemory::InMemory(uint64_t lastIndex)
  : log(Log.GetLogger("raft")),
    entrySliceSize_(settings::Soft::ins().InMemEntrySliceSize),
    minEntrySliceFreeSize_(settings::Soft::ins().MinEntrySliceFreeSize),
    shrunk_(false),
    snapshot_(),
    entries_(),
    markerIndex_(lastIndex + 1),
    appliedToIndex_(),
    appliedToTerm_(),
    savedTo_(lastIndex)
{}

inline uint64_t InMemory::GetMarkerIndex() const
{
  return markerIndex_;
}

inline void InMemory::CheckMarkerIndex() const
{
  if (!entries_.empty() && entries_[0]->index() != markerIndex_) {
    throw Error(ErrorCode::OutOfRange, log,
      "InMemory: marker={}, first={}", markerIndex_, entries_[0]->index());
  }
}

inline void InMemory::CheckBound(uint64_t low, uint64_t high) const
{
  uint64_t upperBound = markerIndex_ + entries_.size();
  if (low > high || low < markerIndex_ || high > upperBound) {
    throw Error(ErrorCode::OutOfRange, log,
      "InMemory: invalid range for entries, low={}, high={}, marker={},"
      " upperBound={}", low, high, markerIndex_, upperBound);
  }
}

inline size_t InMemory::GetEntriesSize() const
{
  return entries_.size();
}

void InMemory::GetEntries(
  EntryVector &entries,
  uint64_t low,
  uint64_t high) const
{
  CheckBound(low, high);
  auto st = entries_.begin() + low - markerIndex_;
  auto ed = entries_.begin() + high - markerIndex_;
  entries.insert(entries.end(), st, ed);
}

void InMemory::GetEntries(
  EntryVector &entries,
  uint64_t low,
  uint64_t high,
  uint64_t maxSize) const
{
  CheckBound(low, high);
  auto st = entries_.begin() + low - markerIndex_;
  auto ed = entries_.begin() + high - markerIndex_;
  uint64_t size = settings::EntryNonCmdSize + (*st)->cmd().size();
  // st + 1 to ensure that GetEntries return at least 1 entry
  // (even this entry larger than maxSize)
  for (auto it = st + 1; it != ed; ++it) {
    size += settings::EntryNonCmdSize + (*it)->cmd().size();
    if (size > maxSize) {
      entries.insert(entries.end(), st, it);
      return;
    }
  }
  entries.insert(entries.end(), st, ed);
}

inline bool InMemory::HasSnapshot() const
{
  return snapshot_ != nullptr;
}

inline pbSnapshotSPtr InMemory::GetSnapshot() const
{
  return snapshot_;
}

StatusWith<uint64_t> InMemory::GetSnapshotIndex() const
{
  if (snapshot_) {
    return snapshot_->index();
  } else {
    return ErrorCode::SnapshotUnavailable;
  }
}

StatusWith<uint64_t> InMemory::GetLastIndex() const
{
  if (!entries_.empty()) {
    return entries_.back()->index();
  } else {
    return GetSnapshotIndex();
  }
}

StatusWith<uint64_t> InMemory::GetTerm(uint64_t index) const
{
  if (index > 0 && index == appliedToIndex_) {
    if (appliedToTerm_ != 0) {
      return appliedToTerm_;
    } else {
      throw Error(ErrorCode::OutOfRange, log,
        "InMemory: appliedToIndex={}, appliedToTerm={}",
        appliedToIndex_, appliedToTerm_);
    }
  }
  if (index < markerIndex_) {
    if (snapshot_ && snapshot_->index() == index) {
      return snapshot_->term();
    } else {
      return ErrorCode::OutOfRange;
    }
  }
  if (!entries_.empty() && entries_.back()->index() >= index) {
    return entries_[index - markerIndex_]->term();
  } else {
    return ErrorCode::OutOfRange;
  }
}

void InMemory::CommitUpdate(const pbUpdateCommit &commit)
{
  if (commit.StableLogIndex > 0) {
    SavedLogTo(commit.StableLogIndex, commit.StableLogTerm);
  }
  if (commit.StableSnapshotIndex > 0) {
    SavedSnapshotTo(commit.StableSnapshotIndex);
  }
}

EntryVector InMemory::GetEntriesToSave() const
{
  uint64_t index = savedTo_ + 1;
  if (index - markerIndex_ > entries_.size()) {
    return {};
  }
  return {entries_.begin() + index - markerIndex_, entries_.end()};
}

void InMemory::SavedLogTo(uint64_t index, uint64_t term)
{
  if (index < markerIndex_) {
    return;
  }
  if (entries_.empty()) {
    return;
  }
  if (index > entries_.back()->index() ||
    term != entries_[index - markerIndex_]->term()) {
    return;
  }
  savedTo_ = index;
}

void InMemory::SavedSnapshotTo(uint64_t index)
{
  if (snapshot_ && snapshot_->index() == index) {
    snapshot_.reset();
  } else if (snapshot_) {
    log->warn("snapshot index={} does not match index={}",
      snapshot_->index(), index);
  }
}

void InMemory::AppliedLogTo(uint64_t index)
{
  if (index < markerIndex_ ||
    entries_.empty() ||
    index > entries_.back()->index()) {
    return;
  }
  const pbEntry &lastAppliedEntry = *entries_[index - markerIndex_];
  if (lastAppliedEntry.index() != index) {
    throw Error(ErrorCode::LogMismatch, log,
      "index != last applied entry index");
  }
  appliedToIndex_ = lastAppliedEntry.index();
  appliedToTerm_ = lastAppliedEntry.term();
  uint64_t newMarkerIndex = index + 1;
  shrunk_ = true;
  EntryVector newEntries;
  newEntries.insert(newEntries.end(),
    std::make_move_iterator(entries_.begin() + newMarkerIndex - markerIndex_),
    std::make_move_iterator(entries_.end()));
  entries_.swap(newEntries);
  markerIndex_ = newMarkerIndex;
  ResizeEntrySlice();
  CheckMarkerIndex();
  // FIXME: rate limit
}

inline void InMemory::Resize()
{
  shrunk_ = false;
  if (entries_.size() > entrySliceSize_) {
    entries_.shrink_to_fit();
  } else {
    entries_.reserve(entrySliceSize_);
  }
}

inline void InMemory::TryResize()
{
  if (shrunk_) {
    Resize();
  }
}

inline void InMemory::ResizeEntrySlice()
{
  bool toResize =
    (entries_.capacity() - entries_.size()) > minEntrySliceFreeSize_;
  if (shrunk_ && (entries_.size() <= 1 || toResize)) {
    Resize();
  }
}

void InMemory::Merge(Span<pbEntrySPtr> ents)
{
  uint64_t firstNewIndex = ents[0]->index();
  ResizeEntrySlice();
  if (firstNewIndex == markerIndex_ + entries_.size()) {
    // |2(snapshot)| |3 4 5 6 7 8| + |9 10 11 ...|, do append
    CheckEntriesToAppend(Span<pbEntrySPtr>(entries_), ents);
    entries_.insert(entries_.end(), ents.begin(), ents.end());
    // FIXME: rate limit
  } else if (firstNewIndex <= markerIndex_) {
    // |2(snapshot)| |3(marker) 4 5 6 7 8| + |1 2|, do overwrite
    // |2(snapshot)| |3(empty, marker)| + |3 4 5|, do append
    markerIndex_ = firstNewIndex;
    shrunk_ = false;
    entries_ = ents.ToVector(); // copy
    savedTo_ = firstNewIndex - 1;
    // FIXME: rate limit
  } else {
    // |3 4 5 6 7 8| + |5 6 7 8|, do conficts resolution
    EntryVector existing;
    GetEntries(existing, markerIndex_, firstNewIndex);
    CheckEntriesToAppend(Span<pbEntrySPtr>(entries_), ents);
    shrunk_ = false;
    entries_.swap(existing); // ?
    entries_.insert(entries_.end(), ents.begin(), ents.end());
    savedTo_ = std::min(savedTo_, firstNewIndex - 1);
  }
  CheckMarkerIndex();
}

void InMemory::Restore(pbSnapshotSPtr s)
{
  markerIndex_ = s->index() + 1;
  appliedToIndex_ = s->index();
  appliedToTerm_ = s->term();
  shrunk_ = false;
  entries_.clear();
  savedTo_ = s->index();
  snapshot_ = std::move(s);
  // FIXME: rate limit
}

} // namespace logdb

} // namespace ycrt