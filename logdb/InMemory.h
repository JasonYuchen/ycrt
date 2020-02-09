//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_RAFT_INMEMORY_H_
#define YCRT_RAFT_INMEMORY_H_

#include <vector>
#include "pb/RaftMessage.h"
#include "utils/Utils.h"
#include "settings/Soft.h"

namespace ycrt
{

namespace logdb
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

// for checking log entry continuity
void CheckEntriesToAppend(Span<pbEntrySPtr> existing, Span<pbEntrySPtr> append);

class InMemory {
 public:
  explicit InMemory(uint64_t lastIndex);
  DISALLOW_COPY_AND_ASSIGN(InMemory);

  uint64_t GetMarkerIndex() const;

  void CheckMarkerIndex() const;

  void CheckBound(uint64_t low, uint64_t high) const;

  size_t GetEntriesSize() const;

  void GetEntries(EntryVector &entries, uint64_t low, uint64_t high) const;

  void GetEntries(EntryVector &entries,
    uint64_t low,
    uint64_t high,
    uint64_t maxSize) const;

  bool HasSnapshot() const;

  pbSnapshotSPtr GetSnapshot() const;

  StatusWith<uint64_t> GetSnapshotIndex() const;

  StatusWith<uint64_t> GetLastIndex() const;

  StatusWith<uint64_t> GetTerm(uint64_t index) const;

  void CommitUpdate(const pbUpdateCommit &commit);

  EntryVector GetEntriesToSave() const;

  void SavedLogTo(uint64_t index, uint64_t term);

  void SavedSnapshotTo(uint64_t index);

  void AppliedLogTo(uint64_t index);

  void Resize();

  void TryResize();

  void ResizeEntrySlice();

  void Merge(Span<pbEntrySPtr> ents);

  void Restore(pbSnapshotSPtr s);

 private:
  slogger log;
  uint64_t entrySliceSize_;
  uint64_t minEntrySliceFreeSize_;
  bool shrunk_;
  pbSnapshotSPtr snapshot_;
  EntryVector entries_;
  uint64_t markerIndex_;
  uint64_t appliedToIndex_;
  uint64_t appliedToTerm_;
  uint64_t savedTo_;
  // FIXME: RateLimiter
};
using InMemoryUPtr = std::unique_ptr<InMemory>;

} // namespace logdb

} // namespace ycrt


#endif //YCRT_RAFT_INMEMORY_H_
