//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_YCRT_NODEHOST_H_
#define YCRT_YCRT_NODEHOST_H_

#include <stdint.h>
#include <unordered_map>
#include <string>

#include "pb/RaftMessage.h"

namespace ycrt
{

struct ClusterInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  bool IsLeader;
  bool IsObserver;
  bool IsWitness;
  pbStateMachineType StateMachineType;
  std::unordered_map<uint64_t, std::string> Nodes;
  uint64_t ConfigChangeIndex;
  bool Pending;
};

class NodeHost {
 public:
  NodeHost();
 private:
};

} // namespace ycrt

#endif //YCRT_YCRT_NODEHOST_H_
