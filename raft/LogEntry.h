//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_RAFT_LOGENTRY_H_
#define YCRT_RAFT_LOGENTRY_H_

#include <utility>
#include <memory>
#include <stdint.h>
#include "pb/RaftMessage.h"

#include <rocksdb/db.h>

namespace ycrt
{

namespace raft
{

class LogDB {
 public:
  std::pair<uint64_t, uint64_t> GetRange();
  void SetRange(std::pair<uint64_t, uint64_t>);
  std::pair<StateUPtr, MembershipUPtr> GetNodeState();
  void SetNodeState(StateUPtr);
  
 private:
};
using LogDBSPtr = std::shared_ptr<LogDB>;

class InMemory {
 public:
 private:
};
using InMemorySPtr = std::shared_ptr<InMemory>;

class LogEntry {
 public:
 private:
  LogDBSPtr logDB_;
  InMemorySPtr inMem_;
  uint64_t committed_;
  uint64_t processed_;
};

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGENTRY_H_
