//
// Created by jason on 2020/1/2.
//

#include "Raft.h"
#include "assert.h"
#include "utils/Error.h"
#include "settings/Soft.h"

namespace ycrt
{

namespace raft
{

using namespace std;

const char *StateToString(enum Raft::State state)
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
    cn_(fmt::format("[{0:05d}:{1:05d}]", clusterID_, nodeID_)),
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
    logEntry_(new LogEntry(logdb)),
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
    handlers_(),
    maxEntrySize_(settings::Soft::ins().MaxEntrySize),
    inMemoryGCTimeout_(settings::Soft::ins().InMemGCTimeout)
{
  pair<pbStateSPtr, pbMembershipSPtr> nodeState = logdb->NodeState();
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

void Raft::Handle(pbMessageSPtr &&m)
{
  assert(m);
  if (!onMessageTermNotMatched(*m)) {
    assert(m->term() == 0 || m->term() == term_);
    (this->*handlers_[state_][m->type()])(*m);
  } else {
    log->info("dropped a {0} from {1}, term not matched", m->type(), m->from());
  }
}

void Raft::Handle(pbMessageSPtr &m)
{
  assert(m);
  if (!onMessageTermNotMatched(*m)) {
    assert(m->term() == 0 || m->term() == term_);
    (this->*handlers_[state_][m->type()])(*m);
  } else {
    log->info("dropped a {0} from {1}, term not matched", m->type(), m->from());
  }
}

void Raft::Handle(pbMessage &m)
{
  if (!onMessageTermNotMatched(m)) {
    assert(m.term() == 0 || m.term() == term_);
    (this->*handlers_[state_][m.type()])(m);
  } else {
    log->info("dropped a {0} from {1}, term not matched", m.type(), m.from());
  }
}

void Raft::Handle(pbMessage &&m)
{
  if (!onMessageTermNotMatched(m)) {
    assert(m.term() == 0 || m.term() == term_);
    (this->*handlers_[state_][m.type()])(m);
  } else {
    log->info("dropped a {0} from {1}, term not matched", m.type(), m.from());
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

string Raft::describe()
{
  uint64_t lastIndex = logEntry_->LastIndex();
  StatusWith<uint64_t> term_s = logEntry_->Term(lastIndex);
  if (!term_s.IsOK() && term_s.Code() != errLogCompacted) {
    log->critical("{0} failed to get term with index={1}", cn_, lastIndex);
    term_s.IsOKOrThrow();
  }
  return fmt::format(
    "[first={0},last={1},term={2},commit={3},applied={4}] {5} with term {6}",
    logEntry_->FirstIndex(), lastIndex, term_s.GetOrDefault(0),
    logEntry_->Committed(), logEntry_->Processed(), cn_, term_);
}

void Raft::loadState(const pbStateSPtr &state)
{
  if (state->commit() < logEntry_->Committed()
    || state->commit() > logEntry_->LastIndex()) {
    log->critical("loadState: state out of range, commit={0}, range=[{1},{2}]",
      state->commit(), logEntry_->Committed(), logEntry_->LastIndex());
    throw Error(errOutOfRange, "state out of range");
  }
  logEntry_->SetCommitted(state->commit());
  term_ = state->term();
  vote_ = state->vote();
}

bool Raft::isFollower()
{
  return state_ == Follower;
}

bool Raft::isPreCandidate()
{
  return state_ == PreCandidate;
}

bool Raft::isCandidate()
{
  return state_ == Candidate;
}

bool Raft::isLeader()
{
  return state_ == Leader;
}

bool Raft::isObserver()
{
  return state_ == Observer;
}

bool Raft::isWitness()
{
  return state_ == Witness;
}

void Raft::mustBeOrThrow(State state)
{
  if (state_ != state) {
    log->critical("{0} is not a {1}", describe(), StateToString(state));
    throw Error(errUnexpectedRaftState);
  }
}

void Raft::mustNotBeOrThrow(State state)
{
  if (state_ == state) {
    log->critical("{0} is a {1}", describe(), StateToString(state));
    throw Error(errUnexpectedRaftState);
  }
}

uint64_t Raft::quorum()
{
  return numVotingMembers()/2 + 1;
}

uint64_t Raft::numVotingMembers()
{
  return remotes_.size() + witnesses_.size();
}

void Raft::resetMatchValueArray()
{
  matched_ = vector<uint64_t>(numVotingMembers(), 0);
}

bool Raft::isSingleNodeQuorum()
{
  return quorum() == 1;
}

bool Raft::leaderHasQuorum()
{
  uint64_t count = 0;
  for (auto &node : getVotingMembers()) {
    if (node.first == nodeID_ || node.second.IsActive()) {
      count++;
      node.second.SetActive(false);
    }
  }
  return count >= quorum();
}

vector<uint64_t> Raft::getNodes()
{
  vector<uint64_t> nodes;
  nodes.reserve(remotes_.size()+observers_.size()+witnesses_.size());
  for(auto &i : remotes_) {
    nodes.emplace_back(i.first);
  }
  for(auto &i : observers_) {
    nodes.emplace_back(i.first);
  }
  for(auto &i : witnesses_) {
    nodes.emplace_back(i.first);
  }
  return nodes;
}

vector<uint64_t> Raft::getSortedNodes()
{
  auto nodes = getNodes();
  std::sort(nodes.begin(), nodes.end());
  return nodes;
}

unordered_map<uint64_t, Remote&> Raft::getVotingMembers()
{
  unordered_map<uint64_t, Remote&> nodes;
  for (auto &i : remotes_) {
    nodes.insert({i.first, i.second});
  }
  for (auto &i : witnesses_) {
    nodes.insert({i.first, i.second});
  }
  return nodes;
}

void Raft::tick()
{
  quiesce_ = false;
  tickCount_++;
  if (timeForInMemoryGC()) {
    logEntry_->InMemoryGC();
  }
  if (isLeader()) {
    leaderTick();
  } else {
    nonLeaderTick();
  }
}

void Raft::leaderTick()
{
  mustBeOrThrow(Leader);
  electionTick_++;
  // TODO: rate limit check
  bool shouldAbortLeaderTransfer = timeForAbortLeaderTransfer();
  if (timeForCheckQuorum()) {
    electionTick_ = 0;
    if (checkQuorum_) {
      pbMessage m;
      m.set_from(nodeID_);
      m.set_type(raftpb::CheckQuorum);
      Handle(m);
    }
  }
  if (shouldAbortLeaderTransfer) {
    abortLeaderTransfer();
  }
  heartbeatTick_++;
  if (timeForHeartbeat()) {
    heartbeatTick_ = 0;
    pbMessage m;
    m.set_from(nodeID_);
    m.set_type(raftpb::LeaderHeartbeat);
    Handle(m);
  }
}

void Raft::nonLeaderTick()
{
  mustNotBeOrThrow(Leader);
  electionTick_++;
  // TODO: rate limit check
  if (isObserver() || isWitness()) {
    return;
  }
  if (!selfRemoved() && timeForElection()) {
    electionTick_ = 0;
    pbMessage m;
    m.set_from(nodeID_);
    m.set_type(raftpb::Election);
    Handle(m);
  }
}

void Raft::quiescedTick()
{
  if (!quiesce_) {
    quiesce_ = true;
    // FIXME r.log.inmem.resize()
  }
  electionTick_++;
}

bool Raft::timeForElection()
{
  return electionTick_ >= randomizedElectionTimeout_;
}

bool Raft::timeForHeartbeat()
{
  return heartbeatTick_ >= heartbeatTimeout_;
}

bool Raft::timeForCheckQuorum()
{
  return electionTick_ >= electionTimeout_;
}

bool Raft::timeForAbortLeaderTransfer()
{
  return isLeaderTransferring() && electionTick_ >= electionTimeout_;
}

bool Raft::timeForInMemoryGC()
{
  return tickCount_ % inMemoryGCTimeout_ == 0;
}

} // namespace raft

} // namespace ycrt