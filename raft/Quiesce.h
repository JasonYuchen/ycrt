//
// Created by jason on 2020/2/11.
//

#ifndef YCRT_RAFT_QUIESCE_H_
#define YCRT_RAFT_QUIESCE_H_

#include <stdint.h>
#include <atomic>

#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace raft
{

class QuiesceManager {
 public:
  QuiesceManager(NodeInfo node, bool enable, uint64_t electionTick);
  bool NewQuiesceState();
  uint64_t IncreaseQuiesceTick();
  void RecordActivity(pbMessageType type);
  void TryEnterQuiesce();
 private:
  bool isQuiesced() const;
  slogger log;
  NodeInfo node_;
  uint64_t tick_;
  uint64_t electionTick_;
  uint64_t quiescedThreshold_;
  uint64_t quiescedSince_;
  uint64_t noActivitySince_;
  uint64_t exitQuiesceTick_;
  std::atomic_bool newQuiesceStateFlag_;
  bool enabled_;
};

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_QUIESCE_H_
