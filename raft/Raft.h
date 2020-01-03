//
// Created by jason on 2020/1/2.
//

#ifndef YCRT_RAFT_RAFT_H_
#define YCRT_RAFT_RAFT_H_

#include <stdint.h>
#include <vector>

namespace ycrt
{

namespace raft
{

class Raft {
 public:
  enum State : int32_t {
    Follower, PreCandidate, Candidate, Leader, Observer, Witness
  };
  struct Status {
    uint64_t ClusterID;
    uint64_t NodeID;
    uint64_t LeaderID;
    uint64_t Applied;
    State NodeState;
    bool IsLeader() { return NodeState == Leader; }
    bool IsFollower() { return NodeState == Follower; }
  };
  Status GetLocalStatus();

 private:
  uint64_t clusterID_;
  uint64_t nodeID_;
  uint64_t leaderID_;
  uint64_t leaderTransferTargetID_;
  bool isLeaderTransfer_;
  bool pendingConfigChange_;
  State state_;
  uint64_t term_;
  uint64_t vote_;
  uint64_t applied_;
  std::vector<uint64_t> matched_;
  bool quiesce_;
  bool checkQuorum_;
  uint64_t tickCount_;
  uint64_t electionTick_;
  uint64_t heartbeatTick_;
  uint64_t electionTimeout_;
  uint64_t heartbeatTimeout_;
  uint64_t randomizedElectionTimeout_;
};

inline const char *StateToString(Raft::State state)
{
  switch (state) {
    case Raft::Follower:
      return "Follower";
    case Raft::PreCandidate:
      return "PreCandidate";
    case Raft::Candidate:
      return "Candidate";
    case Raft::Leader:
      return "Leader";
    case Raft::Observer:
      return "Observer";
    case Raft::Witness:
      return "Witness";
    default:
      return "Unknown";
  }
}

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_RAFT_H_
