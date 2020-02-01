//
// Created by jason on 2020/1/2.
//

#include <random>
#include "Raft.h"
#include <assert.h>
#include "utils/Error.h"
#include "settings/Soft.h"

namespace ycrt
{

namespace raft
{

using namespace std;

Raft::Raft(const Config &config, LogReaderSPtr logdb)
  : log(Log.GetLogger("raft")),
    clusterID_(config.ClusterID),
    nodeID_(config.NodeID),
    cn_(FmtClusterNode(clusterID_, nodeID_)),
    leaderID_(NoLeader),
    leaderTransferTargetID_(NoLeader),
    isLeaderTransferTarget_(false),
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
    logEntry_(new LogEntry(std::move(logdb))),
    readIndex_(),
    readyToRead_(),
    droppedEntries_(),
    droppedReadIndexes_(),
    quiesce_(false),
    checkQuorum_(config.CheckQuorum),
    tickCount_(0),
    electionTick_(0),
    heartbeatTick_(0),
    electionTimeout_(config.ElectionRTT),
    heartbeatTimeout_(config.HeartbeatRTT),
    randomizedElectionTimeout_(0),
    maxEntrySize_(settings::Soft::ins().MaxEntrySize),
    inMemoryGCTimeout_(settings::Soft::ins().InMemGCTimeout),
    randomEngine_(chrono::system_clock::now().time_since_epoch().count()),
    handlers_()
{
  pair<pbStateSPtr, pbMembershipSPtr> nodeState = logdb->NodeState();
  for (auto &node : nodeState.second->addresses()) {
    remotes_.insert({node.first, Remote{}});
    remotes_[node.first].SetNext(1);
  }
  for (auto &node : nodeState.second->observers()) {
    observers_.insert({node.first, Remote{}});
    observers_[node.first].SetNext(1);
  }
  for (auto &node : nodeState.second->witnesses()) {
    witnesses_.insert({node.first, Remote{}});
    witnesses_[node.first].SetNext(1);
  }
  resetMatchValueArray();
  if (!(nodeState.first == nullptr)) {
    loadState(nodeState.first);
  }
  if (config.IsObserver) {
    state_ = Observer;
    becomeObserver(term_, NoLeader);
  } else if (config.IsWitness) {
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

void Raft::Handle(pbMessage &m)
{
  if (!onMessageTermNotMatched(m)) {
    assert(m.term() == 0 || m.term() == term_);
    if(handlers_[state_][m.type()]) {
      (this->*handlers_[state_][m.type()])(m);
    }
  } else {
    log->info("dropped a {0} from {1}, term not matched", m.type(), m.from());
  }
}

void Raft::Handle(pbMessage &&m)
{
  Handle(m);
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

string Raft::describe() const noexcept
{
  uint64_t lastIndex = logEntry_->LastIndex();
  StatusWith<uint64_t> term = logEntry_->Term(lastIndex);
  if (!term.IsOK()) {
    log->critical("{0} failed to get term with index={1} due to {2}", cn_, lastIndex, term.Code());
  }
  return fmt::format(
    "Raft[first={0},last={1},term={2},commit={3},applied={4}] {5} with term {6}",
    logEntry_->FirstIndex(), lastIndex, term.GetOrDefault(0),
    logEntry_->Committed(), logEntry_->Processed(), cn_, term_);
}

pbState Raft::raftState()
{
  pbState state;
  state.set_term(term_);
  state.set_vote(vote_);
  state.set_commit(logEntry_->Committed());
  return state;
}

void Raft::loadState(const pbStateSPtr &state)
{
  if (state->commit() < logEntry_->Committed()
    || state->commit() > logEntry_->LastIndex()) {
    throw Error(ErrorCode::OutOfRange, log,
      "loadState: state out of range, commit={0}, range=[{1},{2}]",
      state->commit(), logEntry_->Committed(), logEntry_->LastIndex());
  }
  logEntry_->SetCommitted(state->commit());
  term_ = state->term();
  vote_ = state->vote();
}

void Raft::setLeaderID(uint64_t leader)
{
  leaderID_ = leader;
  if (listener_) {
    auto info = server::LeaderInfo{};
    info.ClusterID = clusterID_;
    info.NodeID = nodeID_;
    info.LeaderID = leaderID_;
    info.Term = term_;
    listener_->LeaderUpdated(info);
  }
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
    throw Error(ErrorCode::UnexpectedRaftState, log,
      "{0} is not a {1}", describe(), StateToString(state));
  }
}

void Raft::mustNotBeOrThrow(State state)
{
  if (state_ == state) {
    throw Error(ErrorCode::UnexpectedRaftState, log,
      "{0} is a {1}", describe(), StateToString(state));
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

bool Raft::isLeaderTransferring()
{
  return leaderTransferTargetID_ != NoNode && isLeader();
}

bool Raft::leaderHasQuorum()
{
  uint64_t count = 0;
  for (auto &node : getVotingMembers()) {
    if (node.first == nodeID_ || node.second.Active()) {
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

Remote *Raft::getRemoteByNodeID(uint64_t nodeID, bool must)
{
  if (remotes_.find(nodeID) != remotes_.end()) {
    return &remotes_[nodeID];
  } else if (observers_.find(nodeID) != observers_.end()) {
    return &observers_[nodeID];
  } else if (witnesses_.find(nodeID) != witnesses_.end()) {
    return &witnesses_[nodeID];
  } else if (must) {
    throw Error(ErrorCode::RemoteState, log,
      "can not determine the Remote by nodeID={0}", nodeID);
  }
  log->info("{0}: remote {1} not found", describe(), nodeID);
  return nullptr;
}

void Raft::tick()
{
  quiesce_ = false;
  tickCount_++;
  if (timeForInMemoryGC()) {
    logEntry_->InMemoryTryResize();
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
    logEntry_->InMemoryResize();
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

void Raft::setRandomizedElectionTimeout()
{
  randomizedElectionTimeout_ =
    electionTimeout_ + randomEngine_() % electionTimeout_;
}

void Raft::leaderIsAvailable()
{
  electionTick_ = 0;
}

void Raft::send(pbMessageUPtr m)
{
  assert(m);
  m->set_from(nodeID_);
  finalizeMessageTerm(*m);
  messages_.emplace_back(std::move(m));
}

void Raft::finalizeMessageTerm(pbMessage &m)
{
  if (m.term() == 0 && m.type() == raftpb::RequestVote) {
    throw Error(ErrorCode::UnexpectedRaftMessage, log,
      "{0}: sending a RequestVote with 0 term", describe());
  }
  if (m.term() > 0 && m.type() != raftpb::RequestVote) {
    throw Error(ErrorCode::UnexpectedRaftMessage, log,
      "{0}: term is unexpectedly set for message {1}", describe(), m.DebugString());
  }
  if (!isRequestMessage(m.type())) {
    m.set_term(term_);
  }
}

void Raft::sendReplicateMessage(uint64_t to)
{
  Remote *remote = getRemoteByNodeID(to, true);
  if (remote->IsPaused()) {
    return;
  }
  auto m = makeReplicateMessage(to, remote->Next(), maxEntrySize_);
  if (m == nullptr) { // log compaction or other error
    if (!remote->Active()) {
      log->warn("{0}: Remote={1} is not active, skip sending snapshot", describe(), to);
      return;
    }
    m = makeInstallSnapshotMessage(to);
    // FIXME: error msg?
    log->info("{0}: start sending snapshot with index={1} to {2}", describe(), m->snapshot().index(), *remote);
    remote->BecomeSnapshot(m->snapshot().index());
  } else {
    if (!m->entries().empty()) {
      remote->Progress(m->entries().rbegin()->index());
    }
  }
  send(std::move(m));
}

void Raft::broadcastReplicateMessage()
{
  mustBeOrThrow(Leader);
  for (auto &node : getNodes()) {
    sendReplicateMessage(node);
  }
}

void Raft::sendHeartbeatMessage(uint64_t to, pbSystemCtx hint, uint64_t match)
{
  uint64_t commit = min(match, logEntry_->Committed());
  auto m = make_unique<pbMessage>();
  m->set_to(to);
  m->set_type(raftpb::Heartbeat);
  m->set_commit(commit);
  m->set_hint(hint.Low);
  m->set_hint_high(hint.High);
  send(std::move(m));
}

void Raft::broadcastHeartbeatMessage()
{
  mustBeOrThrow(Leader);
  if (readIndex_.HasPendingRequest()) {
    broadcastHeartbeatMessage(readIndex_.BackCtx());
  } else {
    broadcastHeartbeatMessage({0, 0});
  }
}

void Raft::broadcastHeartbeatMessage(pbSystemCtx hint)
{
  auto zeroHint = pbSystemCtx{0, 0};
  for (auto &node : getVotingMembers()) {
    if (node.first != nodeID_) {
      sendHeartbeatMessage(node.first, hint, node.second.Match());
    }
  }
  if (hint == zeroHint) {
    for (auto &node : observers_) {
      sendHeartbeatMessage(node.first, hint, node.second.Match());
    }
  }
}

void Raft::sendTimeoutNowMessage(uint64_t to)
{
  auto m = make_unique<pbMessage>();
  m->set_to(to);
  m->set_type(raftpb::TimeoutNow);
  send(std::move(m));
}

pbMessageUPtr Raft::makeInstallSnapshotMessage(uint64_t to)
{
  auto m = make_unique<pbMessage>();
  m->set_to(to);
  m->set_type(raftpb::InstallSnapshot);
  pbSnapshotSPtr snapshot = logEntry_->GetSnapshot();
  if (snapshot == nullptr) {
    throw Error(ErrorCode::SnapshotUnavailable, log,
      "{0}: got an empty snapshot", describe());
  }
  if (witnesses_.find(to) != witnesses_.end()) {
    finalizeWitnessSnapshot(*snapshot);
  }
  // FIXME
  m->set_allocated_snapshot(new pbSnapshot(*snapshot));
}

pbMessageUPtr Raft::makeReplicateMessage(
  uint64_t to,
  uint64_t next,
  uint64_t maxSize)
{
  StatusWith<uint64_t> _term = logEntry_->Term(next - 1);
  if (_term.Code() == ErrorCode::LogCompacted) {
    return nullptr;
  }
  uint64_t term = _term.GetOrThrow();
  StatusWith<vector<pbEntry>> _entries =
    logEntry_->GetEntriesFromStart(next, maxSize);
  if (_entries.Code() == ErrorCode::LogCompacted) {
    return nullptr;
  }
  vector<pbEntry> &entries = _entries.GetMutableOrThrow();
  if (!entries.empty()) {
    uint64_t lastIndex = entries.rbegin()->index();
    uint64_t expected = entries.size() + next - 1;
    if (lastIndex != expected) {
      throw Error(ErrorCode::LogMismatch, log,
        "{0}: Replicate expected {1}, actual {2}", describe(), expected, lastIndex);
    }
  }
  if (witnesses_.find(to) != witnesses_.end()) {
    auto meta = makeMetadataEntries(entries);
    std::swap(entries, meta);
  }
  // TODO: performance?
  auto m = make_unique<pbMessage>();
  m->set_to(to);
  m->set_type(raftpb::Replicate);
  m->set_log_index(next - 1);
  m->set_log_term(term);
  for (auto &item : entries) {
    m->mutable_entries()->Add(std::move(item));
  }
  m->set_commit(logEntry_->Committed());
  return m;
}

vector<pbEntry> Raft::makeMetadataEntries(const vector<pbEntry> &entries)
{
  vector<pbEntry> meta;
  for (auto &entry : entries) {
    if (entry.type() != raftpb::ConfigChangeEntry) {
      meta.emplace_back(pbEntry{});
      meta.back().set_type(raftpb::MetadataEntry);
      meta.back().set_index(entry.index());
      meta.back().set_term(entry.term());
    } else {
      meta.push_back(entry);
    }
  }
  return meta;
}

void Raft::finalizeWitnessSnapshot(pbSnapshot &s)
{
  s.set_filepath("");
  s.set_file_size(0);
  s.clear_files();
  s.set_witness(true);
  s.set_dummy(false);
}

void Raft::reportDroppedConfigChange(pbEntry &&e)
{
  droppedEntries_.emplace_back(std::move(e));
}

void Raft::reportDroppedProposal(pbMessage &m)
{
  for (auto &entry : m.entries()) {
    droppedEntries_.emplace_back(entry);
  }
  if (listener_) {
    auto info = server::ProposalInfo{};
    info.ClusterID = clusterID_;
    info.NodeID = nodeID_;
    for (auto &entry : m.entries()) {
      info.Entries.emplace_back(entry);
    }
    listener_->ProposalDropped(info);
  }
}

void Raft::reportDroppedReadIndex(pbMessage &m)
{
  droppedReadIndexes_.emplace_back(pbSystemCtx{m.hint(), m.hint_high()});
  if (listener_) {
    auto info = server::ReadIndexInfo{};
    info.ClusterID = clusterID_;
    info.NodeID = nodeID_;
    listener_->ReadIndexDropped(info);
  }
}

void Raft::sortMatchValues()
{
  std::sort(matched_.begin(), matched_.end());
}

bool Raft::tryCommit()
{
  mustBeOrThrow(Leader);
  if (numVotingMembers() != matched_.size()) {
    resetMatchValueArray();
  }
  size_t index = 0;
  for (auto &node : remotes_) {
    matched_[index++] = node.second.Match();
  }
  for (auto &node : witnesses_) {
    matched_[index++] = node.second.Match();
  }
  sortMatchValues();
  // commit as large index as possible
  uint64_t q = matched_[numVotingMembers() - quorum()];
  return logEntry_->TryCommit(q, term_);
}

void Raft::appendEntries(Span<pbEntry> entries)
{
  uint64_t lastIndex = logEntry_->LastIndex();
  for (auto &entry : entries) {
    entry.set_term(term_);
    entry.set_index(1 + lastIndex++);
  }
  logEntry_->Append(entries);
  remotes_[nodeID_].TryUpdate(logEntry_->LastIndex());
  if (isSingleNodeQuorum()) {
    tryCommit();
  }
}

void Raft::reset(uint64_t term)
{
  if (term_ != term) {
    term_ = term;
    vote_ = NoLeader;
  }
  // FIXME ratelimit
  votes_.clear();
  electionTick_ = 0;
  heartbeatTick_ = 0;
  setRandomizedElectionTimeout();
  readIndex_.Clear();
  pendingConfigChange_ = false;
  abortLeaderTransfer();
  resetRemotes();
  resetObservers();
  resetWitnesses();
  resetMatchValueArray();
}

void Raft::resetRemotes()
{
  for (auto &node : remotes_) {
    node.second = Remote{};
    node.second.SetNext(logEntry_->LastIndex() + 1);
    if (node.first == nodeID_) {
      node.second.SetMatch(logEntry_->LastIndex());
    }
  }
}

void Raft::resetObservers()
{
  for (auto &node : observers_) {
    node.second = Remote{};
    node.second.SetNext(logEntry_->LastIndex() + 1);
    if (node.first == nodeID_) {
      node.second.SetMatch(logEntry_->LastIndex());
    }
  }
}

void Raft::resetWitnesses()
{
  for (auto &node : witnesses_) {
    node.second = Remote{};
    node.second.SetNext(logEntry_->LastIndex() + 1);
    if (node.first == nodeID_) {
      node.second.SetMatch(logEntry_->LastIndex());
    }
  }
}

void Raft::becomeObserver(uint64_t term, uint64_t leaderID)
{
  if (!isObserver()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "only observer can transfer to observer");
  }
  reset(term);
  setLeaderID(leaderID);
  log->info("{0}: became an observer", describe());
}

void Raft::becomeWitness(uint64_t term, uint64_t leaderID)
{
  if (!isWitness()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "only witness can transfer to witness");
  }
  reset(term);
  setLeaderID(leaderID);
  log->info("{0}: became a witness", describe());
}

void Raft::becomeFollower(uint64_t term, uint64_t leaderID)
{
  if (isWitness()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "witness cannot transfer to follower");
  }
  state_ = Follower;
  reset(term);
  setLeaderID(leaderID);
  log->info("{0}: became a follower", describe());
}

void Raft::becomePreCandidate()
{
  throw Error(ErrorCode::UnexpectedRaftState, "unimplemented");
}

void Raft::becomeCandidate()
{
  if (isLeader()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "leader cannot transfer to candidate");
  }
  if (isObserver()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "observer cannot transfer to candidate");
  }
  if (isWitness()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "witness cannot transfer to candidate");
  }
  // prevote?
  reset(term_ + 1);
  setLeaderID(NoLeader);
  vote_ = nodeID_;
  log->info("{0}: became a candidate", describe());
}

void Raft::becomeLeader()
{
  if (!isLeader() && !isCandidate()) {
    throw Error(ErrorCode::UnexpectedRaftState, log, "only leader or candidate can transfer to leader");
  }
  state_ = Leader;
  reset(term_);
  setLeaderID(nodeID_);
  preLeaderPromotionHandleConfigChange();
  vector<pbEntry> null{pbEntry{}};
  null[0].set_type(raftpb::ApplicationEntry);
  appendEntries({null.data(), null.size()});
  log->info("{0}: became a leader", describe());
}

bool Raft::restore(const pbSnapshot &s)
{
  if (s.index() <= logEntry_->Committed()) {
    log->info("{0}: snapshot index={1} < committed={2}", describe(), s.index(), logEntry_->Committed());
    return false;
  }
  if (!isObserver()) {
    for (auto &node : s.membership().observers()) {
      if (node.first == nodeID_) {
        throw Error(ErrorCode::UnexpectedRaftState, log, "{0}: converting to observer, received snapshot {1}", s.DebugString());
      }
    }
  }
  if (!isWitness()) {
    for (auto &node : s.membership().witnesses()) {
      if (node.first == nodeID_) {
        throw Error(ErrorCode::UnexpectedRaftState, log, "{0}: converting to witness, received snapshot {1}", s.DebugString());
      }
    }
  }
  if (logEntry_->MatchTerm(s.index(), s.term())) {
    // a snapshot with index X implies log entry X has been committed
    logEntry_->CommitTo(s.index());
    return false;
  }
  log->info("{0}: start to restore snapshot with index={1} and term={2}", describe(), s.index(), s.term());
  logEntry_->Restore(s);
  return true;
}

void Raft::restoreRemotes(const pbSnapshot &s)
{
  uint64_t match = 0;
  uint64_t next = logEntry_->LastIndex() + 1;

  // restore full members
  remotes_.clear();
  for (auto &node : s.membership().addresses()) {
    // promote observer
    if (node.first == nodeID_ && isObserver()) {
      becomeFollower(term_, leaderID_);
    }
    if (witnesses_.find(node.first) != witnesses_.end()) {
      throw Error(ErrorCode::UnexpectedRaftState, "witness should not be promoted to a full member");
    }
    match = 0;
    if (node.first == nodeID_) {
      match = next - 1;
    }
    setRemote(node.first, match, next);
    log->info("{0}: remote {1} is restored with {2}", describe(), node.first, remotes_[node.first]);
  }
  if (selfRemoved() && isLeader()) {
    becomeFollower(term_, NoLeader);
  }

  // restore observers
  observers_.clear();
  for (auto &node : s.membership().observers()) {
    match = 0;
    if (node.first == nodeID_) {
      match = next - 1;
    }
    setObserver(node.first, match, next);
    log->info("{0}: observer {1} is restores with {2}", describe(), node.first, observers_[node.first]);
  }

  // restore witnesses
  witnesses_.clear();
  for (auto &node : s.membership().witnesses()) {
    match = 0;
    if (node.first == nodeID_) {
      match = next - 1;
    }
    setWitness(node.first, match, next);
    log->info("{0}: witness {1} is restores with {2}", describe(), node.first, witnesses_[node.first]);
  }
  resetMatchValueArray();
}

void Raft::campaign()
{
  log->info("{0}: campaign, num of voting members is {1}", describe(), numVotingMembers());
  becomeCandidate();
  uint64_t term = term_;
  if (listener_) {
    auto info = server::CampaignInfo{};
    info.ClusterID = clusterID_;
    info.NodeID = nodeID_;
    info.Term = term;
    listener_->CampaignLaunched(info);
  }
  handleVoteResp(nodeID_, false);
  if (isSingleNodeQuorum()) {
    becomeLeader();
    return;
  }
  uint64_t hint = NoNode;
  // if current node is the target node of the leader transfer
  if (isLeaderTransferTarget_) {
    hint = nodeID_;
    isLeaderTransferTarget_ = false;
  }
  for (auto &node : getVotingMembers()) {
    if (node.first == nodeID_) {
      continue;
    }
    auto m = make_unique<pbMessage>();
    m->set_term(term);
    m->set_to(node.first);
    m->set_type(raftpb::RequestVote);
    m->set_log_index(logEntry_->LastIndex());
    m->set_term(logEntry_->LastTerm());
    m->set_hint(hint);
    send(std::move(m));
    log->info("{0}: send RequestVote to node {1}", describe(), node.first);
  }
}

uint64_t Raft::handleVoteResp(uint64_t from, bool rejected)
{
  if (rejected) {
    log->info("{0}: received RequestVoteResp, rejection from {1} at term {2}", describe(), from, term_);
  }  else {
    log->info("{0}: received RequestVoteResp, vote granted from {1} at term {2}", describe(), from, term_);
  }
  uint64_t numOfVotesGranted = 0;
  if (votes_.find(from) == votes_.end()) {
    votes_[from] = !rejected;
  }
  for (auto &vote : votes_) {
    if (vote.second) {
      numOfVotesGranted++;
    }
  }
  return numOfVotesGranted;
}

bool Raft::canGrantVote(pbMessage &m)
{
  return vote_ == NoNode || vote_ == m.from() || m.term() > term_;
}

bool Raft::selfRemoved()
{
  if (isObserver()) {
    return observers_.find(nodeID_) == observers_.end();
  }
  if (isWitness()) {
    return witnesses_.find(nodeID_) == witnesses_.end();
  }
  return remotes_.find(nodeID_) == remotes_.end();
}

void Raft::addNode(uint64_t nodeID)
{
  pendingConfigChange_ = false;
  if (nodeID_ == nodeID && isWitness()) {
    throw Error(ErrorCode::UnexpectedRaftState, "{0}: is a witness and cannot be added", describe());
  }
  if (remotes_.find(nodeID) != remotes_.end()) {
    // already
    return;
  }
  if (observers_.find(nodeID) != observers_.end()) {
    // promotion from observer
    remotes_[nodeID] = observers_[nodeID];
    observers_.erase(nodeID);
    if (nodeID_ == nodeID) {
      becomeFollower(term_, leaderID_);
    }
  } else if (witnesses_.find(nodeID) != witnesses_.end()) {
    // promotion from witness is not allowed
    throw Error(ErrorCode::UnexpectedRaftState, "{0}: could not promote witness", describe());
  } else {
    // normal add new node
    setRemote(nodeID, 0, logEntry_->LastIndex() + 1);
  }
}

void Raft::addObserver(uint64_t nodeID)
{
  pendingConfigChange_ = false;
  if (nodeID_ == nodeID && !isObserver()) {
    throw Error(ErrorCode::UnexpectedRaftState, "{0}: is not an observer", describe());
  }
  if (observers_.find(nodeID) != observers_.end()) {
    // already
    return;
  }
  setObserver(nodeID, 0, logEntry_->LastIndex() + 1);
}

void Raft::addWitness(uint64_t nodeID)
{
  pendingConfigChange_ = false;
  if (nodeID_ == nodeID && !isWitness()) {
    throw Error(ErrorCode::UnexpectedRaftState, "{0}: is not a witness", describe());
  }
  if (witnesses_.find(nodeID) != witnesses_.end()) {
    // already
    return;
  }
  setWitness(nodeID, 0, logEntry_->LastIndex() + 1);
}

void Raft::removeNode(uint64_t nodeID)
{
  pendingConfigChange_ = false;
  remotes_.erase(nodeID);
  observers_.erase(nodeID);
  witnesses_.erase(nodeID);
  if (nodeID_ == nodeID && isLeader()) {
    becomeFollower(term_, NoLeader);
  }
  if (isLeaderTransferring() && leaderTransferTargetID_ == nodeID) {
    abortLeaderTransfer();
  }
  // maybe the lagged is removed, try to advance
  if (isLeader() && numVotingMembers() > 0) {
    if (tryCommit()) {
      broadcastReplicateMessage();
    }
  }
}

void Raft::setRemote(uint64_t nodeID, uint64_t match, uint64_t next)
{
  log->info("{0}: set remote, Node={1}, Match={2}, Next={3}", describe(), nodeID, match, next);
  auto remote = Remote{}.SetMatch(match).SetNext(next);
  remotes_.insert({nodeID, remote});
}

void Raft::setObserver(uint64_t nodeID, uint64_t match, uint64_t next)
{
  log->info("{0}: set observer, Node={1}, Match={2}, Next={3}", describe(), nodeID, match, next);
  auto remote = Remote{}.SetMatch(match).SetNext(next);
  observers_.insert({nodeID, remote});
}

void Raft::setWitness(uint64_t nodeID, uint64_t match, uint64_t next)
{
  log->info("{0}: set witness, Node={1}, Match={2}, Next={3}", describe(), nodeID, match, next);
  auto remote = Remote{}.SetMatch(match).SetNext(next);
  witnesses_.insert({nodeID, remote});
}

uint64_t Raft::getPendingConfigChangeCount()
{
  uint64_t index = logEntry_->Committed() + 1;
  uint64_t count = 0;
  while (true) {
    auto entries = logEntry_->GetEntriesFromStart(index, maxEntrySize_);
    if (entries.GetOrDefault({}).empty()) {
      return count;
    }
    for (auto &entry : entries.GetOrThrow()) {
      if (entry.type() == raftpb::ConfigChangeEntry) {
        count++;
      }
    }
    index = entries.GetOrThrow().back().index() + 1;
  }
}

void Raft::preLeaderPromotionHandleConfigChange()
{
  uint64_t count = getPendingConfigChangeCount();
  if (count > 1) {
    throw Error(ErrorCode::UnexpectedRaftState, "{0}: multiple pending ConfigChange found", describe());
  } else if (count == 1) {
    log->info("{0}: becoming leader with pending ConfigChange", describe());
    pendingConfigChange_ = true;
  }
}

bool Raft::hasConfigChangeToApply()
{
  // TODO: scan all committed but not applied logs to find out
  //  whether there is any ConfigChange entry
  return logEntry_->Committed() > applied_;
}

void Raft::abortLeaderTransfer()
{
  leaderTransferTargetID_ = NoNode;
}

void Raft::handleHeartbeat(pbMessage &m)
{
  logEntry_->CommitTo(m.commit());
  auto resp = make_unique<pbMessage>();
  resp->set_to(m.from());
  resp->set_type(raftpb::HeartbeatResp);
  resp->set_hint(m.hint());
  resp->set_hint_high(m.hint_high());
  send(std::move(resp));
}

void Raft::handleInstallSnapshot(pbMessage &m)
{
  log->info("{0}: InstallSnapshot received from {1}", describe(), m.from());
  uint64_t index = m.snapshot().index();
  uint64_t term = m.snapshot().term();
  auto resp = make_unique<pbMessage>();
  resp->set_to(m.from());
  resp->set_type(raftpb::ReplicateResp);
  if (restore(m.snapshot())) {
    log->info("{0}: restored snapshot with index={1} and term={2}", describe(), index, term);
    resp->set_log_index(logEntry_->LastIndex());
  } else {
    log->info("{0}: rejected snapshot with index={1} and term={2}", describe(), index, term);
    resp->set_log_index(logEntry_->Committed());
    if (listener_) {
      auto info = server::SnapshotInfo{};
      info.ClusterID = clusterID_;
      info.NodeID = nodeID_;
      info.Index = index;
      info.Term = term;
      info.From = m.from();
      listener_->SnapshotRejected(info);
    }
  }
  send(std::move(resp));
}

void Raft::handleReplicate(pbMessage &m)
{
  auto resp = make_unique<pbMessage>();
  resp->set_to(m.from());
  resp->set_type(raftpb::ReplicateResp);
  if (m.log_index() < logEntry_->Committed()) {
    resp->set_log_index(logEntry_->Committed());
    send(std::move(resp));
    return;
  }
  if (logEntry_->MatchTerm(m.log_index(), m.log_term())) {
    // FIXME : remove ent
    vector<pbEntry> ent;
    for (auto &entry : m.entries()) {
      ent.emplace_back(entry);
    }
    logEntry_->TryAppend(m.log_index(), {ent.data(), ent.size()});
    uint64_t lastIndex = m.log_index() + m.entries().size();
    logEntry_->CommitTo(min(lastIndex, m.commit()));
    resp->set_log_index(lastIndex);
  } else {
    log->warn("{0}: rejected replicate with index={1} and term={2} from node {3}", describe(), m.log_index(), m.term(), m.from());
    resp->set_reject(true);
    resp->set_log_index(m.log_index());
    resp->set_hint(logEntry_->LastIndex());
    if (listener_) {
      auto info = server::ReplicationInfo{};
      info.ClusterID = clusterID_;
      info.NodeID = nodeID_;
      info.Index = m.log_index();
      info.From = m.from();
      listener_->ReplicationRejected(info);
    }
  }
  send(std::move(resp));
}

bool Raft::isRequestMessage(pbMessageType type)
{
  return
    type == raftpb::Propose ||
    type == raftpb::ReadIndex;
}

bool Raft::isLeaderMessage(pbMessageType type)
{
  return
    type == raftpb::Replicate ||
    type == raftpb::InstallSnapshot ||
    type == raftpb::Heartbeat ||
    type == raftpb::TimeoutNow ||
    type == raftpb::ReadIndexResp;
}

// by dropping the RequestVote from high term node
// when we do not exceed the minimum election timeout
// we can minimize the interruption by network partition problem
// Alternative approach: PreVote mechanism
bool Raft::dropRequestVoteFromHighTermNode(pbMessage &m)
{
  if (m.type() != raftpb::RequestVote || !checkQuorum_ || m.term() < term_) {
    return false;
  }
  if (m.hint() == m.from()) {
    log->info("{0}: RequestVote with leader transfer hint received from {1}", describe(), m.from());
    return false;
  }
  if (isLeader() && !quiesce_ && electionTick_ >= electionTimeout_) {
    throw Error(ErrorCode::UnexpectedRaftState, "{0}: electionTick >= electionTimeout on leader detected", describe());
  }
  return leaderID_ != NoLeader && electionTick_ < electionTimeout_;
}

bool Raft::onMessageTermNotMatched(pbMessage &m)
{
  if (m.term() == 0 || m.term() == term_) {
    return false;
  }
  if (dropRequestVoteFromHighTermNode(m)) {
    log->info("{0}: dropped a RequestVote from {1} with term {2}", describe(), m.from(), m.term());
    return true;
  }
  if (m.term() > term_) {
    log->info("{0}: received a {1} from {2} with higher term={3}", describe(), m.type(), m.from(), m.term());
    uint64_t leaderID = NoLeader;
    if (isLeaderMessage(m.type())) {
      leaderID = m.from();
    }
    if (isObserver()) {
      becomeObserver(m.term(), leaderID);
    } else if (isWitness()) {
      becomeWitness(m.term(), leaderID);
    } else {
      becomeFollower(m.term(), leaderID);
    }
  } else if (m.term() < term_) {
    if (isLeaderMessage(m.type()) && checkQuorum_) {
      // this corner case is documented in the following etcd test
      // TestFreeStuckCandidateWithCheckQuorum
      //
      // When network partition recovers, the isolated node(C) with higher term
      // rejoins the cluster, the other nodes(A, B) will ignore the RequestVote
      // message from C due to the existing leader(A).
      // Then A will send messages to the C which triggers C sending the NoOP
      // The A receives the NoOP with higher term and then steps down to
      // follower waiting for a next election.
      // But C (if timeouts and become candidate) lost some log entries so A, B
      // will not cast votes.
      // Then one of the A, B timeouts, start a new election and win
      auto resp = make_unique<pbMessage>();
      resp->set_to(m.from());
      resp->set_type(raftpb::NoOP);
      send(std::move(resp));
    } else {
      log->info("{0}: ignored a {1} from {2} with lower term={3}", describe(), m.type(), m.from(), m.term());
    }
    return true;
  }
  return false;
}

void Raft::handleElection(pbMessage &m)
{
  if (!isLeader()) {
    // there can be multiple pending membership change entries committed but not
    // applied on this node. say with a cluster of X, Y and Z, there are two
    // such entries for adding node A and B are committed but not applied
    // available on X. If X is allowed to start a new election, it can become the
    // leader with a vote from any one of the node Y or Z. Further proposals made
    // by the new leader X in the next term will require a quorum of 2 which can
    // have no overlap with the committed quorum of 3. this violates the safety
    // requirement of raft.
    // ignore the Election message when there is membership configure change
    // committed but not applied
    if (hasConfigChangeToApply()) {
      log->warn("{0}: election skipped due to pending ConfigChange", describe());
      if (listener_) {
        auto info = server::CampaignInfo{};
        info.ClusterID = clusterID_;
        info.NodeID = nodeID_;
        info.Term = term_;
        listener_->CampaignSkipped(info);
      }
    }
    log->info("{0}: start election at term={1}", describe(), term_);
    campaign();
  } else {
    log->info("{0}: election ignored at leader node");
  }
}

void Raft::handleRequestVote(pbMessage &m)
{
  auto resp = make_unique<pbMessage>();
  resp->set_to(m.from());
  resp->set_type(raftpb::RequestVoteResp);
  bool canGrant = canGrantVote(m);
  bool isLogUpToDate = logEntry_->IsUpToDate(m.log_index(), m.log_term());
  if (canGrant && isLogUpToDate) {
    log->info("{0}: cast vote from {1} with index={2}, term={3} and log term={4}", describe(), m.from(), m.log_index(), m.term(), m.log_term());
    electionTick_ = 0;
    vote_ = m.from();
  } else {
    log->info("{0}: rejected vote request from {1} with index={2}, term={3} and log term={4} due to canGrant={5}, upToDate={6}", describe(), m.from(), m.log_index(), m.term(), m.log_term(), canGrant, isLogUpToDate);
    resp->set_reject(true);
  }
  send(std::move(resp));
}

void Raft::handleConfigChange(pbMessage &m)
{
  if (m.reject()) {
    pendingConfigChange_ = false;
  } else {
    // use hint_high to store the change type of a ConfigChange
    uint64_t changeType = static_cast<raftpb::ConfigChangeType>(m.hint_high());
    // use hint to store the added/removed nodeID
    uint64_t nodeID = m.hint();
    switch (changeType) {
      case raftpb::AddNode: addNode(nodeID);
      case raftpb::RemoveNode: removeNode(nodeID);
      case raftpb::AddObserver: addObserver(nodeID);
      case raftpb::AddWitness: addWitness(nodeID);
      default:
        throw Error(ErrorCode::UnexpectedRaftMessage, "unexpected ConfigChangeType={0}", changeType);
    }
  }
}

void Raft::handleLocalTick(pbMessage &m)
{
  if (m.reject()) {
    quiescedTick();
  } else {
    tick();
  }
}

void Raft::handleRestoreRemote(pbMessage &m)
{
  restoreRemotes(m.snapshot());
}

void Raft::handleLeaderPropose(pbMessage &m)
{
  mustBeOrThrow(Leader);
  if (isLeaderTransferring()) {
    log->warn("{0}: dropped a proposal, leader transferring", describe());
    reportDroppedProposal(m);
    return;
  }
  for (auto &entry : *m.mutable_entries()) {
    if (entry.type() == raftpb::ConfigChangeEntry) {
      if (pendingConfigChange_) {
        log->warn("{0}: dropped an extra ConfigChange", describe());
        reportDroppedConfigChange(std::move(entry));
        entry = pbEntry{};
        entry.set_type(raftpb::ApplicationEntry);
      }
      pendingConfigChange_ = true;
    }
  }
  // FIXME: remove ent
  vector<pbEntry> ent;
  for (auto &entry : m.entries()) {
    ent.emplace_back(entry);
  }
  appendEntries({ent.data(), ent.size()});
  broadcastReplicateMessage();
}

void Raft::handleLeaderHeartbeat(pbMessage &m)
{
  mustBeOrThrow(Leader);
  broadcastHeartbeatMessage();
}

void Raft::handleLeaderCheckQuorum(pbMessage &m)
{
  mustBeOrThrow(Leader);
  if (!leaderHasQuorum()) {
    log->warn("{0}: lost quorum", describe());
    becomeFollower(term_, NoLeader);
  }
}

bool Raft::hasCommittedEntryAtCurrentTerm()
{
  if (term_ == 0) {
    throw Error(ErrorCode::UnexpectedRaftState, "not supposed to reach here");
  }
  StatusWith<uint64_t> lastCommittedTerm = logEntry_->Term(logEntry_->Committed());
  if (!lastCommittedTerm.IsOK() && lastCommittedTerm.Code() == ErrorCode::LogCompacted) {
    return true;
  }
  return lastCommittedTerm.GetOrDefault(0) == term_;
}

void Raft::handleLeaderReadIndex(pbMessage &m)
{
  mustBeOrThrow(Leader);
  auto ctx = pbSystemCtx{};
  ctx.Low = m.hint();
  ctx.High = m.hint_high();
  if (witnesses_.find(m.from()) != witnesses_.end()) {
    log->error("{0}: dropped ReadIndex from witness {1}", describe(), m.from());
  } else if (!isSingleNodeQuorum()) {
    if (!hasCommittedEntryAtCurrentTerm()) {
      // leader doesn't know the commit value of the cluster
      // see raft thesis section 6.4, this is the first step of the ReadIndex
      // protocol.
      log->warn("{0}: dropped ReadIndex, leader is not ready", describe());
      reportDroppedReadIndex(m);
      return;
    }
    readIndex_.AddRequest(logEntry_->Committed(), ctx, m.from());
    broadcastHeartbeatMessage(ctx);
  } else {
    readyToRead_.emplace_back(pbReadyToRead{logEntry_->Committed(), ctx});
    if (m.from() != nodeID_ && observers_.find(m.from()) != observers_.end()) {
      auto resp = make_unique<pbMessage>();
      resp->set_to(m.from());
      resp->set_type(raftpb::ReadIndexResp);
      resp->set_log_index(logEntry_->Committed());
      resp->set_hint(m.hint());
      resp->set_hint_high(m.hint_high());
      resp->set_commit(m.commit());
      send(std::move(resp));
    }
  }
}

void Raft::handleLeaderReplicateResp(pbMessage &m)
{
  mustBeOrThrow(Leader);
  auto remote = getRemoteByNodeID(m.from(), false);
  if (!remote) {
    return;
  }
  remote->SetActive(true);
  if (!m.reject()) {
    bool paused = remote->IsPaused();
    if (remote->TryUpdate(m.log_index())) {
      remote->RespondedTo();
      if (tryCommit()) {
        broadcastReplicateMessage();
      } else if (paused) {
        sendReplicateMessage(m.from());
      }
      // TODO: TO READ: according to the leadership transfer protocol listed on the p29 of the
      //  raft thesis
      if (isLeaderTransferring() &&
        m.from() == leaderTransferTargetID_ &&
        logEntry_->LastIndex() == remote->Match()) {
        sendTimeoutNowMessage(leaderTransferTargetID_);
      }
    }
  } else {
    // the replication flow control code is derived from etcd raft, it resets
    // nextIndex to match + 1. it is thus even more conservative than the raft
    // thesis's approach of nextIndex = nextIndex - 1 mentioned on the p21 of
    // the thesis.
    if (remote->DecreaseTo(m.log_index(), m.hint())) {
      remote->EnterRetry();
      sendReplicateMessage(m.from());
    }
  }

}

void Raft::handleLeaderHeartbeatResp(pbMessage &m)
{
  mustBeOrThrow(Leader);
  auto remote = getRemoteByNodeID(m.from(), false);
  if (!remote) {
    return;
  }
  remote->SetActive(true);
  remote->WaitToRetry();
  if (remote->Match() < logEntry_->LastIndex()) {
    sendReplicateMessage(m.from());
  }
  // heartbeat response contains leadership confirmation requested as part of
  // the ReadIndex protocol.
  if (m.hint() != 0) {
    handleLeaderReadIndexConfirmation(m);
  }
}

void Raft::handleLeaderLeaderTransfer(pbMessage &m)
{
  mustBeOrThrow(Leader);
  auto remote = getRemoteByNodeID(m.from(), false);
  if (!remote) {
    return;
  }
  // use hint to store the target of the leader transfer request
  uint64_t target = m.hint();
  log->info("{0}: LeaderTransfer, target={1}", describe(), target);
  if (target == NoNode) {
    throw Error(ErrorCode::UnexpectedRaftMessage, "LeaderTransfer target not set");
  }
  if (isLeaderTransferring()) {
    log->warn("{0}: ignored LeaderTransfer, transfer is ongoing", describe());
    return;
  }
  if (nodeID_ == target) {
    log->warn("{0}: ignored LeaderTransfer, target is itself", describe());
    return;
  }
  leaderTransferTargetID_ = target;
  electionTick_ = 0;
  // fast path below, if the log entry is consistent, timeout now to start election
  // or wait for the target node to catch up, see p29 of the raft thesis
  if (remote->Match() == logEntry_->LastIndex()) {
    sendTimeoutNowMessage(target);
  }
}

void Raft::handleLeaderReadIndexConfirmation(pbMessage &m)
{
  auto ctx = pbSystemCtx{};
  ctx.Low = m.hint();
  ctx.High = m.hint_high();
  vector<ReadStatus> readStatus = readIndex_.Confirm(ctx, m.from(), quorum());
  for (auto &s : readStatus) {
    if (s.From == NoNode || s.From == nodeID_) {
      readyToRead_.emplace_back(pbReadyToRead{s.Index, s.Ctx});
    } else {
      auto resp = make_unique<pbMessage>();
      resp->set_to(s.From);
      resp->set_type(raftpb::ReadIndexResp);
      resp->set_log_index(s.Index);
      resp->set_hint(m.hint());
      resp->set_hint_high(m.hint_high());
      send(std::move(resp));
    }
  }
}

void Raft::handleLeaderSnapshotStatus(pbMessage &m)
{
  auto remote = getRemoteByNodeID(m.from(), false);
  if (!remote) {
    return;
  }
  if (remote->State() != Remote::Snapshot) {
    return;
  }
  if (m.reject()) {
    remote->ClearPendingSnapshot();
    log->info("{0}: snapshot failed, remote {1} is now in wait state", describe(), m.from());
  } else {
    log->info("{0}: snapshot succeeded, remote {1} is now in wait state, next={2}", describe(), m.from(), remote->Next());
  }
  remote->BecomeWait();
}

void Raft::handleLeaderUnreachable(pbMessage &m)
{
  if(!getRemoteByNodeID(m.from(), false)) {
    return;
  }
  log->info("{0}: received Unreachable, remote {1} entered retry state", describe(), m.from());
}

// FIXME handleLeaderRateLimit


// re-route observer handler to follower handler
void Raft::handleObserverPropose(pbMessage &m)
{
  handleFollowerPropose(m);
}

void Raft::handleObserverReplicate(pbMessage &m)
{
  handleFollowerReplicate(m);
}

void Raft::handleObserverHeartbeat(pbMessage &m)
{
  handleFollowerHeartbeat(m);
}

void Raft::handleObserverInstallSnapshot(pbMessage &m)
{
  handleFollowerInstallSnapshot(m);
}

void Raft::handleObserverReadIndex(pbMessage &m)
{
  handleFollowerReadIndex(m);
}

void Raft::handleObserverReadIndexResp(pbMessage &m)
{
  handleFollowerReadIndexResp(m);
}

// re-route witness handler to follower handler
void Raft::handleWitnessReplicate(pbMessage &m)
{
  handleFollowerReplicate(m);
}

void Raft::handleWitnessHeartbeat(pbMessage &m)
{
  handleFollowerHeartbeat(m);
}

void Raft::handleWitnessInstallSnapshot(pbMessage &m)
{
  handleFollowerInstallSnapshot(m);
}

void Raft::handleFollowerPropose(pbMessage &m)
{
  if (leaderID_ == NoLeader) {
    log->warn("{0}: dropped proposal as no available leader", describe());
    reportDroppedProposal(m);
  } else {
    // FIXME: do not copy
    auto resp = make_unique<pbMessage>(m);
    resp->set_to(leaderID_);
    send(std::move(resp));
  }
}

void Raft::handleFollowerReplicate(pbMessage &m)
{
  leaderIsAvailable();
  setLeaderID(m.from());
  handleReplicate(m);
}

void Raft::handleFollowerHeartbeat(pbMessage &m)
{
  leaderIsAvailable();
  setLeaderID(m.from());
  handleHeartbeat(m);
}

void Raft::handleFollowerReadIndex(pbMessage &m)
{
  if (leaderID_ == NoLeader) {
    log->warn("{0}: dropped ReadIndex as no available leader", describe());
    reportDroppedReadIndex(m);
  } else {
    // FIXME: do not copy
    auto resp = make_unique<pbMessage>(m);
    resp->set_to(leaderID_);
    send(std::move(resp));
  }
}

void Raft::handleFollowerLeaderTransfer(pbMessage &m)
{
  if (leaderID_ == NoLeader) {
    log->warn("{0}: dropped LeadTransfer as no available leader", describe());
  } else {
    // FIXME: do not copy
    auto resp = make_unique<pbMessage>(m);
    resp->set_to(leaderID_);
    send(std::move(resp));
  }
}

void Raft::handleFollowerReadIndexResp(pbMessage &m)
{
  auto ctx = pbSystemCtx{};
  ctx.Low = m.hint();
  ctx.High = m.hint_high();
  leaderIsAvailable();
  setLeaderID(m.from());
  readyToRead_.emplace_back(pbReadyToRead{m.log_index(), ctx});
}

void Raft::handleFollowerInstallSnapshot(pbMessage &m)
{
  leaderIsAvailable();
  setLeaderID(m.from());
  handleInstallSnapshot(m);
}

void Raft::handleFollowerTimeoutNow(pbMessage &m)
{
  // the last paragraph, p29 of the raft thesis mentions that this is nothing
  // different from the clock moving forward quickly
  log->info("{0}: TimeoutNow received", describe());
  electionTick_ = randomizedElectionTimeout_;
  isLeaderTransferTarget_ = true;
  tick();
  if (isLeaderTransferTarget_) {
    isLeaderTransferTarget_ = false;
  }
}

void Raft::handleCandidatePropose(pbMessage &m)
{
  log->warn("{0}: dropped proposal as no available leader", describe());
  reportDroppedProposal(m);
}

// implies that there is a leader for current term == m.term()
void Raft::handleCandidateReplicate(pbMessage &m)
{
  becomeFollower(term_, m.from());
  handleReplicate(m);
}

// implies that there is a leader for current term == m.term()
void Raft::handleCandidateHeartbeat(pbMessage &m)
{
  becomeFollower(term_, m.from());
  handleHeartbeat(m);
}

void Raft::handleCandidateReadIndex(pbMessage &m)
{
  log->warn("{0}: dropped ReadIndex as no available leader", describe());
  reportDroppedReadIndex(m);
  droppedReadIndexes_.emplace_back(pbSystemCtx{m.hint(), m.hint_high()});
}

// implies that there is a leader for current term == m.term()
void Raft::handleCandidateInstallSnapshot(pbMessage &m)
{
  becomeFollower(term_, m.from());
  handleInstallSnapshot(m);
}

void Raft::handleCandidateRequestVoteResp(pbMessage &m)
{
  if (observers_.find(m.from()) != observers_.end()) {
    log->warn("{0}: dropped RequestVoteResp from observer {1}", m.from());
    return;
  }
  uint64_t count = handleVoteResp(m.from(), m.reject());
  log->info("{0}: received {1} votes and {2} rejections, quorum is {3}", describe(), count, votes_.size() - count, quorum());
  // 3rd paragraph section 5.2 of the raft paper
  if (count == quorum()) {
    becomeLeader();
    broadcastReplicateMessage();
  } else if (votes_.size() - count == quorum()) {
    // etcd raft does this, it is not stated in the raft paper
    becomeFollower(term_, NoLeader);
  }
}

} // namespace raft

} // namespace ycrt