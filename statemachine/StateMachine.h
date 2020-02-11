//
// Created by jason on 2020/2/11.
//

#ifndef YCRT_STATEMACHINE_STATEMACHINE_H_
#define YCRT_STATEMACHINE_STATEMACHINE_H_

#include "pb/RaftMessage.h"
#include "SnapshotIO.h"

namespace ycrt
{

namespace statemachine
{

struct Task {
  uint64_t ClusterID;
  uint64_t NodeID;
  uint64_t Index;
  bool SnapshotAvailable;
  bool InitialSnapshot;
  bool SnapshotRequested;
  bool StreamingSnapshot;
  bool PeriodicSync;
  bool NewNode;
  struct SnapshotRequest SnapshotRequest;
  EntryVector Entries;
};
using TaskSPtr = std::shared_ptr<Task>;

} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_STATEMACHINE_H_
