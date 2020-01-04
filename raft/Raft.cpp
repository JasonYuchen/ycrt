//
// Created by jason on 2020/1/2.
//

#include "Raft.h"

#include "assert.h"

namespace ycrt
{

namespace raft
{

using namespace std;

const char *StateToString(Raft::State state)
{
  static const char *states[] =
    {"Follower", "PreCandidate", "Candidate", "Leader", "Observer", "Witness"};
  assert(state < Raft::NumOfState);
  return states[state];
}

Raft::Raft(ConfigSPtr &config, LogDBSPtr &logdb)
  : log(Log.GetLogger("raft")),
    clusterID_(config->ClusterID),
    nodeID_(config->NodeID),
    leaderID_(NoLeader),
    leaderTransferTargetID_(NoLeader),
    isLeaderTransfer_(false),
    pendingConfigChange_(false),
    state_(Follower),
    term_(0),
    vote_(0),
    applied_(0),
    votes_(),
    remotes_(),
    observers_(),
    witnesses_(),
    messages_(),
    matched_(),
    log_(new LogEntry(logdb)),
    //readIndex_(),
    //readyToRead_(),
    droppedEntries_(),
    //droppedReadIndexes_,
    quiesce_(false),
    checkQuorum_(config->CheckQuorum),
    tickCount_(0),
    electionTick_(0),
    heartbeatTick_(0),
    electionTimeout_(config->ElectionRTT),
    heartbeatTimeout_(config->HeartbeatRTT),
    randomizedElectionTimeout_(0),
    handlers_()
{
  pair<pbStateSPtr, pbMembershipSPtr> nodeState = logdb->GetNodeState();
  for (auto &node : nodeState.second->addresses()) {
    remotes_.insert({node.first, Remote{.Next = 1}});
  }
  for (auto &node : nodeState.second->observers()) {
    observers_.insert({node.first, Remote{.Next = 1}});
  }
  for (auto &node : nodeState.second->witnesses()) {
    witnesses_.insert({node.first, Remote{.Next = 1}});
  }
  resetMatchValueArray();
  if (!(nodeState.first == nullptr)) {
    loadState(nodeState.first);
  }
  if (config->IsObserver) {
    state_ = Observer;
    becomeObserver(term_, NoLeader);
  } else if (config->IsWitness) {
    state_ = Witness;
    becomeWitness(term_, NoLeader);
  } else {
    becomeFollower(term_, NoLeader);
  }
  initializeHandlerMap();
  checkHandlerMapOrThrow();
}

Raft::Status Raft::GetLocalStatus()
{
  return Status{
    .ClusterID = clusterID_,
    .NodeID = nodeID_,
    .LeaderID = leaderID_,
    .Applied = applied_,
    .NodeState = state_};
}

void Raft::Handle(pbMessageSPtr &m)
{
  if (!onMessageTermNotMatched(m)) {
    assert(m->term() == 0 || m->term() == term_);
    (this->*handlers_[state_][m->type()])(m);
  } else {
    log->info("dropped a {0} from {1}, term not matched", m->type(), m->from());
  }
}

void Raft::initializeHandlerMap()
{
  for (auto &states : handlers_) {
    for (auto &handler : states) {
      handler = nullptr;
    }
  }

  handlers_[Candidate][raftpb::Propose] = &Raft::handleCandidatePropose;
  handlers_[Candidate][raftpb::Heartbeat] = &Raft::handleCandidateHeartbeat;
  handlers_[Candidate][raftpb::ReadIndex] = &Raft::handleCandidateReadIndex;
  handlers_[Candidate][raftpb::Replicate] = &Raft::handleCandidateReplicate;
  handlers_[Candidate][raftpb::InstallSnapshot] = &Raft::handleCandidateInstallSnapshot;
  handlers_[Candidate][raftpb::RequestVoteResp] = &Raft::handleCandidateRequestVoteResp;
  handlers_[Candidate][raftpb::Election] = &Raft::handleElection;
  handlers_[Candidate][raftpb::RequestVote] = &Raft::handleRequestVote;
  handlers_[Candidate][raftpb::ConfigChangeEvent] = &Raft::handleConfigChange;
  handlers_[Candidate][raftpb::LocalTick] = &Raft::handleLocalTick;
  handlers_[Candidate][raftpb::SnapshotReceived] = &Raft::handleRestoreRemote;

  handlers_[Follower][raftpb::Propose] = &Raft::handleFollowerPropose;
  handlers_[Follower][raftpb::Replicate] = &Raft::handleFollowerReplicate;
  handlers_[Follower][raftpb::Heartbeat] = &Raft::handleFollowerHeartbeat;
  handlers_[Follower][raftpb::ReadIndex] = &Raft::handleFollowerReadIndex;
  handlers_[Follower][raftpb::LeaderTransfer] = &Raft::handleFollowerLeaderTransfer;
  handlers_[Follower][raftpb::ReadIndexResp] = &Raft::handleFollowerReadIndexResp;
  handlers_[Follower][raftpb::InstallSnapshot] = &Raft::handleFollowerInstallSnapshot;
  handlers_[Follower][raftpb::TimeoutNow] = &Raft::handleFollowerTimeoutNow;
  handlers_[Follower][raftpb::Election] = &Raft::handleElection;
  handlers_[Follower][raftpb::RequestVote] = &Raft::handleRequestVote;
  handlers_[Follower][raftpb::ConfigChangeEvent] = &Raft::handleConfigChange;
  handlers_[Follower][raftpb::LocalTick] = &Raft::handleLocalTick;
  handlers_[Follower][raftpb::SnapshotReceived] = &Raft::handleRestoreRemote;

  handlers_[Leader][raftpb::Propose] = &Raft::handleLeaderPropose;
  handlers_[Leader][raftpb::LeaderHeartbeat] = &Raft::handleLeaderHeartbeat;
  handlers_[Leader][raftpb::CheckQuorum] = &Raft::handleLeaderCheckQuorum;
  handlers_[Leader][raftpb::ReadIndex] = &Raft::handleLeaderReadIndex;
  handlers_[Leader][raftpb::ReplicateResp] = &Raft::handleLeaderReplicateResp;
  handlers_[Leader][raftpb::HeartbeatResp] = &Raft::handleLeaderHeartbeatResp;
  handlers_[Leader][raftpb::SnapshotStatus] = &Raft::handleLeaderSnapshotStatus;
  handlers_[Leader][raftpb::Unreachable] = &Raft::handleLeaderUnreachable;
  handlers_[Leader][raftpb::LeaderTransfer] = &Raft::handleLeaderLeaderTransfer;
  handlers_[Leader][raftpb::Election] = &Raft::handleElection;
  handlers_[Leader][raftpb::RequestVote] = &Raft::handleRequestVote;
  handlers_[Leader][raftpb::ConfigChangeEvent] = &Raft::handleConfigChange;
  handlers_[Leader][raftpb::LocalTick] = &Raft::handleLocalTick;
  handlers_[Leader][raftpb::SnapshotReceived] = &Raft::handleRestoreRemote;
  handlers_[Leader][raftpb::RateLimit]; // FIXME: RateLimit not implemented

  handlers_[Observer][raftpb::Propose] = &Raft::handleObserverPropose;
  handlers_[Observer][raftpb::Replicate] = &Raft::handleObserverReplicate;
  handlers_[Observer][raftpb::InstallSnapshot] = &Raft::handleObserverInstallSnapshot;
  handlers_[Observer][raftpb::Heartbeat] = &Raft::handleObserverHeartbeat;
  handlers_[Observer][raftpb::ReadIndex] = &Raft::handleObserverReadIndex;
  handlers_[Observer][raftpb::ReadIndexResp] = &Raft::handleObserverReadIndexResp;
  handlers_[Observer][raftpb::ConfigChangeEvent] = &Raft::handleConfigChange;
  handlers_[Observer][raftpb::LocalTick] = &Raft::handleLocalTick;
  handlers_[Observer][raftpb::SnapshotReceived] = &Raft::handleRestoreRemote;

  handlers_[Witness][raftpb::Heartbeat] = &Raft::handleWitnessHeartbeat;
  handlers_[Witness][raftpb::Replicate] = &Raft::handleWitnessReplicate;
  handlers_[Witness][raftpb::InstallSnapshot] = &Raft::handleWitnessInstallSnapshot;
  handlers_[Witness][raftpb::RequestVote] = &Raft::handleRequestVote;
  handlers_[Witness][raftpb::ConfigChangeEvent] = &Raft::handleConfigChange;
  handlers_[Witness][raftpb::LocalTick] = &Raft::handleLocalTick;
  handlers_[Witness][raftpb::SnapshotReceived] = &Raft::handleRestoreRemote;
}


} // namespace raft

} // namespace ycrt