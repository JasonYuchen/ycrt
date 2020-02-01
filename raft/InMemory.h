//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_RAFT_INMEMORY_H_
#define YCRT_RAFT_INMEMORY_H_

#include <vector>
#include "pb/RaftMessage.h"
#include "utils/Utils.h"
#include "settings/Soft.h"
#include "LogEntryUtils.h"

namespace ycrt
{

namespace raft
{

// inMemory is a two stage in memory log storage struct to keep log entries
// that will be used by the raft protocol in immediate future.
//
//  |   on disk       |     in memory      |
//  |  0  1  2  3  4  |  3  4  5  6  7  8  |
//  |                 |      |             |
//                       |     |
//              markerIndex  savedTo
//
//
class InMemory {
 public:
  explicit InMemory(uint64_t lastIndex)
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

  uint64_t GetMarkerIndex() const
  {
    return markerIndex_;
  }

  void CheckMarkerIndex() const
  {
    if (!entries_.empty()) {
      if (entries_[0].index() != markerIndex_) {
        throw Error(ErrorCode::OutOfRange,
          "InMemory: marker={0}, first={1}", markerIndex_, entries_[0].index());
      }
    }
  }

  void CheckBound(uint64_t low, uint64_t high) const
  {
    uint64_t upperBound = markerIndex_ + entries_.size();
    if (low > high || low < markerIndex_ || high > upperBound) {
      throw Error(ErrorCode::OutOfRange,
        "InMemory: invalid range for entries, low={0}, high={1}, marker={2},"
        " upperBound={3}", low, high, markerIndex_, upperBound);
    }
  }

  size_t GetEntriesSize() const
  {
    return entries_.size();
  }

  void GetEntries(std::vector<pbEntry> &entries,
    uint64_t low, uint64_t high) const
  {
    CheckBound(low, high);
    auto st = entries_.begin() + low - markerIndex_;
    auto ed = entries_.begin() + high - markerIndex_;
    entries.insert(entries.end(), st, ed);
  }

  void GetEntries(std::vector<pbEntry> &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const
  {
    CheckBound(low, high);
    uint64_t size = 0;
    auto st = entries_.begin() + low - markerIndex_;
    auto ed = entries_.begin() + high - markerIndex_;
    for (auto it = st; it != ed; ++it) {
      size += settings::EntryNonCmdSize + it->cmd().size();
      if (size > maxSize) {
        entries.insert(entries.end(), st, it);
        return;
      }
    }
    entries.insert(entries.end(), st, ed);
  }

  bool HasSnapshot() const
  {
    return snapshot_ != nullptr;
  }

  pbSnapshotSPtr GetSnapshot() const
  {
    return snapshot_;
  }

  StatusWith<uint64_t> GetSnapshotIndex() const
  {
    if (snapshot_) {
      return snapshot_->index();
    } else {
      return ErrorCode::SnapshotUnavailable;
    }
  }

  StatusWith<uint64_t> GetLastIndex() const
  {
    if (!entries_.empty()) {
      return entries_.back().index();
    } else {
      return GetSnapshotIndex();
    }
  }

  StatusWith<uint64_t> GetTerm(uint64_t index) const
  {
    if (index > 0 && index == appliedToIndex_) {
      if (appliedToTerm_ != 0) {
        return appliedToTerm_;
      } else {
        throw Error(ErrorCode::OutOfRange, "InMemory: appliedToIndex={0}, appliedToTerm={1}", appliedToIndex_, appliedToTerm_);
      }
    }

    if (index < markerIndex_) {
      if (snapshot_ && snapshot_->index() == index) {
        return snapshot_->term();
      } else {
        return ErrorCode::OutOfRange;
      }
    }

    if (!entries_.empty() && entries_.back().index() >= index) {
      return entries_[index - markerIndex_].term();
    } else {
      return ErrorCode::OutOfRange;
    }
  }

  void CommitUpdate(const pbUpdateCommit &commit)
  {
    if (commit.StableLogTo > 0) {
      savedLogTo(commit.StableLogTo, commit.StableLogTerm);
    }
    if (commit.StableSnapshotTo > 0) {
      savedSnapshotTo(commit.StableSnapshotTo);
    }
  }

  std::vector<pbEntry> GetEntriesToSave() const
  {
    uint64_t index = savedTo_ + 1;
    if (index - markerIndex_ > entries_.size()) {
      return {};
    }
    return {entries_.begin() + index - markerIndex_, entries_.end()};
  }

  void savedLogTo(uint64_t index, uint64_t term)
  {
    if (index < markerIndex_) {
      return;
    }
    if (entries_.empty()) {
      return;
    }
    if (index > entries_.back().index() ||
      term != entries_[index - markerIndex_].term()) {
      return;
    }
    savedTo_ = index;
  }

  void savedSnapshotTo(uint64_t index)
  {
    if (snapshot_ && snapshot_->index() == index) {
      snapshot_.reset();
    } else if (snapshot_) {
      log->warn("snapshot index={0} does not match index={1}", snapshot_->index(), index);
    }
  }

  void AppliedLogTo(uint64_t index)
  {
    if (index < markerIndex_) {
      return;
    }
    if (entries_.empty()) {
      return;
    }
    if (index > entries_.back().index()) {
      return;
    }
    const pbEntry &lastAppliedEntry = entries_[index - markerIndex_];
    if (lastAppliedEntry.index() != index) {
      throw Error(ErrorCode::LogMismatch, "index != last applied entry index");
    }
    appliedToIndex_ = lastAppliedEntry.index();
    appliedToTerm_ = lastAppliedEntry.term();
    uint64_t newMarkerIndex = index + 1;
    shrunk_ = true;
    entries_ = std::vector<pbEntry>(entries_.begin() + newMarkerIndex - markerIndex_, entries_.end());
    markerIndex_ = newMarkerIndex;
    ResizeEntrySlice();
    CheckMarkerIndex();
    // FIXME: rate limit
  }

  void Resize()
  {
    shrunk_ = false;
    entries_.shrink_to_fit();
  }

  void TryResize()
  {
    if (shrunk_) {
      Resize();
    }
  }

  void ResizeEntrySlice()
  {
    bool toResize = (entries_.capacity() - entries_.size()) > minEntrySliceFreeSize_;
    if (shrunk_ && (entries_.size() <= 1 || toResize)) {
      Resize();
    }
  }

  void Merge(const Span<pbEntry> ents)
  {
    uint64_t firstNewIndex = ents[0].index();
    ResizeEntrySlice();
    if (firstNewIndex == markerIndex_ + entries_.size()) {
      // |2(snapshot)| |3 4 5 6 7 8| + |9 10 11 ...|, do append
      CheckEntriesToAppend(Span<pbEntry>(entries_), ents);
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
      std::vector<pbEntry> existing;
      GetEntries(existing, markerIndex_, firstNewIndex);
      CheckEntriesToAppend(Span<pbEntry>(entries_), ents);
      shrunk_ = false;
      entries_.swap(existing); // ?
      entries_.insert(entries_.end(), ents.begin(), ents.end());
      savedTo_ = std::min(savedTo_, firstNewIndex - 1);
    }
    CheckMarkerIndex();
  }

  void Restore(pbSnapshotSPtr s)
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

 private:
  slogger log;
  uint64_t entrySliceSize_;
  uint64_t minEntrySliceFreeSize_;
  bool shrunk_;
  pbSnapshotSPtr snapshot_;
  std::vector<pbEntry> entries_;
  uint64_t markerIndex_;
  uint64_t appliedToIndex_;
  uint64_t appliedToTerm_;
  uint64_t savedTo_;
  // FIXME: RateLimiter
};
using InMemoryUPtr = std::unique_ptr<InMemory>;

} // namespace raft

} // namespace ycrt


#endif //YCRT_RAFT_INMEMORY_H_
