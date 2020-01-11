//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_RAFT_INMEMORY_H_
#define YCRT_RAFT_INMEMORY_H_

#include <vector>
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace raft
{

class InMemory {
 public:
  InMemory(uint64_t lastIndex)
    : shrunk_(false),
      newEntries_(true),
      snapshot_(),
      entries_(),
      markerIndex_(lastIndex + 1),
      savedTo_(lastIndex) {}
 private:
  bool shrunk_;
  bool newEntries_;
  pbSnapshotUPtr snapshot_;
  std::vector<pbEntry> entries_;
  uint64_t markerIndex_;
  uint64_t savedTo_;
};
using InMemorySPtr = std::shared_ptr<InMemory>;

} // namespace raft

} // namespace ycrt


#endif //YCRT_RAFT_INMEMORY_H_
