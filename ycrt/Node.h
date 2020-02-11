//
// Created by jason on 2020/1/14.
//

#ifndef YCRT_YCRT_NODE_H_
#define YCRT_YCRT_NODE_H_

#include <stdint.h>
#include <string>
#include <functional>

#include "Config.h"
#include "statemachine/StateMachine.h"
#include "server/Message.h"
#include "raft/Peer.h"
#include "ExecEngine.h"
#include "transport/Transport.h"

namespace ycrt
{

struct ClusterInfo;

class Node {
 public:
 private:

  const uint64_t incomingProposalsMaxLen_;
  const uint64_t incomingReadIndexMaxLen_;
  const uint64_t syncTaskInterval_;
  const uint64_t lazyFreeCycle_;

  uint64_t readRequestCount_;
  uint64_t leaderID_;
  uint64_t instanceID_;
  std::string raftAddress_;
  ConfigSPtr config_;
  BlockingConcurrentQueueSPtr<pbConfigChangeSPtr> configChangeQueue_;
  BlockingConcurrentQueueSPtr<statemachine::SnapshotRequestSPtr> snapshotQueue_;
  BlockingConcurrentQueueSPtr<statemachine::TaskSPtr> taskQueue_;
  BlockingConcurrentQueueSPtr<pbMessageUPtr> raftMessages_;
  uint64_t appliedIndex_; // applied to statemachine
  uint64_t confirmedIndex_; //
  uint64_t pushedIndex_;
  ExecEngine &engine_;
  // TODO: get stream connection
  std::function<void(uint64_t, uint64_t, bool)> handleSnapshotStatus_;
  // TODO: consider Transport& ?
  std::function<void(pbMessageUPtr)> sendRaftMessage_;
  // TODO: statemachine::StateMachine stateMachine_;
  pbStateMachineType stateMachineType_;
  BlockingConcurrentQueueSPtr<pbEntrySPtr> incomingProposals_;
  // BlockingConcurrentQueueSPtr readIndexes_;
  // pendingProposals_;
  // pendingReadIndexes_;
  // pendingConfigChanges_;
  // pendingSnapshots_;
  // pendingLeaderTransfers_;
  std::mutex raftMutex_;
  raft::PeerUPtr peer_;
  logdb::LogReaderSPtr logReader_;
  logdb::LogDBSPtr logDB_;
  // snapshotter
  transport::NodeResolver nodeResolver_;
  std::atomic_bool stopped_;
  // FIXME: cpp20 std::atomic_shared_ptr<>
  //  std::atomic<std::shared_ptr<ClusterInfo>> clusterInfo;
};

} // namespace ycrt

#endif //YCRT_YCRT_NODE_H_

