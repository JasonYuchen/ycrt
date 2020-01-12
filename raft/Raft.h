//
// Created by jason on 2020/1/2.
//

#ifndef YCRT_RAFT_RAFT_H_
#define YCRT_RAFT_RAFT_H_

#include <stdint.h>
#include <vector>
#include <assert.h>
#include <unordered_map>
#include <functional>
#include <random>

#include "server/Event.h"
#include "utils/Utils.h"
#include "Remote.h"
#include "pb/RaftMessage.h"
#include "LogEntry.h"
#include "ycrt/Config.h"
#include "server/Event.h"

#include "tests/TestHack.h"

namespace ycrt
{

namespace raft
{

class Raft {
 public:
  enum State : uint8_t {
    Follower = 0, PreCandidate, Candidate, Leader, Observer, Witness, NumOfState
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
  Raft(ConfigSPtr &config, LogDBSPtr &logdb);
  Status GetLocalStatus();
  void Handle(pbMessage &&m);
  void Handle(pbMessage &m);
 private:
  void initializeHandlerMap();

  // status
  std::string describe();
  void loadState(const pbStateSPtr &state);
  void setLeaderID(uint64_t leader);
  bool isFollower();
  bool isPreCandidate();
  bool isCandidate();
  bool isLeader();
  bool isObserver();
  bool isWitness();
  void mustBeOrThrow(State);
  void mustNotBeOrThrow(State);
  uint64_t quorum();
  uint64_t numVotingMembers();
  void resetMatchValueArray();
  bool isSingleNodeQuorum();
  bool isLeaderTransferring();
  bool leaderHasQuorum();
  std::vector<uint64_t> getNodes();
  std::vector<uint64_t> getSortedNodes();
  std::unordered_map<uint64_t, Remote&> getVotingMembers();
  Remote *getRemoteByNodeID(uint64_t nodeID, bool must);

  // tick
  void tick();
  void leaderTick();
  void nonLeaderTick();
  void quiescedTick();
  bool timeForElection();
  bool timeForHeartbeat();
  bool timeForCheckQuorum();
  bool timeForAbortLeaderTransfer();
  bool timeForInMemoryGC();
  // TODO: bool timeForRateLimitCheck();
  void setRandomizedElectionTimeout();
  void leaderIsAvailable();

  // send
  void send(pbMessageUPtr m);
  void finalizeMessageTerm(pbMessage &m);
  void sendReplicateMessage(uint64_t to);
  void broadcastReplicateMessage();
  void sendHeartbeatMessage(uint64_t to ,pbSystemCtx hint, uint64_t match);
  void broadcastHeartbeatMessage();
  void broadcastHeartbeatMessage(pbSystemCtx hint);
  void sendTimeoutNowMessage(uint64_t to);

  // message generation
  pbMessageUPtr makeInstallSnapshotMessage(uint64_t to);
  pbMessageUPtr makeReplicateMessage(uint64_t to, uint64_t next, uint64_t maxSize);
  std::vector<pbEntry> makeMetadataEntries(const std::vector<pbEntry> &entries);
  void finalizeWitnessSnapshot(pbSnapshot &s);

  // message dropped
  void reportDroppedConfigChange(pbEntry &&e);
  void reportDroppedProposal(pbMessage &m);
  void reportDroppedReadIndex(pbMessage &m);

  // log append and commit
  void sortMatchValues();
  bool tryCommit();
  void appendEntries(Span<pbEntry> entries);

  // state transition
  void reset(uint64_t term);
  void resetRemotes();
  void resetObservers();
  void resetWitnesses();
  void becomeObserver(uint64_t term, uint64_t leaderID);
  void becomeWitness(uint64_t term, uint64_t leaderID);
  void becomeFollower(uint64_t term, uint64_t leaderID);
  void becomePreCandidate();
  void becomeCandidate();
  void becomeLeader();
  bool restore(const pbSnapshot &s);
  void restoreRemotes(const pbSnapshot &s);

  // election
  void campaign();
  uint64_t handleVoteResp(uint64_t from, bool rejected);
  bool canGrantVote(pbMessage &m);

  // membership
  bool selfRemoved();
  void addNode(uint64_t nodeID);
  void addObserver(uint64_t nodeID);
  void addWitness(uint64_t nodeID);
  void removeNode(uint64_t nodeID);
  void setRemote(uint64_t nodeID, uint64_t match, uint64_t next);
  void setObserver(uint64_t nodeID, uint64_t match, uint64_t next);
  void setWitness(uint64_t nodeID, uint64_t match, uint64_t next);
  uint64_t getPendingConfigChangeCount();
  void preLeaderPromotionHandleConfigChange();
  bool hasConfigChangeToApply();
  void abortLeaderTransfer();

  // handlers
  void handleHeartbeat(pbMessage &m);
  void handleInstallSnapshot(pbMessage &m);
  void handleReplicate(pbMessage &m);
  bool isRequestMessage(pbMessageType type);
  bool isLeaderMessage(pbMessageType type);
  bool dropRequestVoteFromHighTermNode(pbMessage &m);
  bool onMessageTermNotMatched(pbMessage &m);
  void handleElection(pbMessage &m);
  void handleRequestVote(pbMessage &m);
  void handleConfigChange(pbMessage &m);
  void handleLocalTick(pbMessage &m);
  void handleRestoreRemote(pbMessage &m);

  // handlers in Leader node
  void handleLeaderPropose(pbMessage &m);
  void handleLeaderHeartbeat(pbMessage &m);
  void handleLeaderCheckQuorum(pbMessage &m);
  bool hasCommittedEntryAtCurrentTerm();
  // TODO: addReadyToread
  void handleLeaderReadIndex(pbMessage &m);
  void handleLeaderReplicateResp(pbMessage &m);
  void handleLeaderHeartbeatResp(pbMessage &m);
  void handleLeaderLeaderTransfer(pbMessage &m);
  void handleReadIndexLeaderConfirmation(pbMessage &m);
  void handleLeaderSnapshotStatus(pbMessage &m);
  void handleLeaderUnreachable(pbMessage &m);

  // handlers in Observer - re-route to follower's handlers
  void handleObserverPropose(pbMessage &m);
  void handleObserverReplicate(pbMessage &m);
  void handleObserverHeartbeat(pbMessage &m);
  void handleObserverInstallSnapshot(pbMessage &m);
  void handleObserverReadIndex(pbMessage &m);
  void handleObserverReadIndexResp(pbMessage &m);

  // handlers in witness - re-route to follower's handlers
  void handleWitnessReplicate(pbMessage &m);
  void handleWitnessHeartbeat(pbMessage &m);
  void handleWitnessInstallSnapshot(pbMessage &m);

  // handlers in follower
  void handleFollowerPropose(pbMessage &m);
  void handleFollowerReplicate(pbMessage &m);
  void handleFollowerHeartbeat(pbMessage &m);
  void handleFollowerReadIndex(pbMessage &m);
  void handleFollowerLeaderTransfer(pbMessage &m);
  void handleFollowerReadIndexResp(pbMessage &m);
  void handleFollowerInstallSnapshot(pbMessage &m);
  void handleFollowerTimeoutNow(pbMessage &m);

  // handlers in candidate
  void handleCandidatePropose(pbMessage &m);
  void handleCandidateReplicate(pbMessage &m);
  void handleCandidateHeartbeat(pbMessage &m);
  void handleCandidateReadIndex(pbMessage &m);
  void handleCandidateInstallSnapshot(pbMessage &m);
  void handleCandidateRequestVoteResp(pbMessage &m);

  slogger log;
  uint64_t clusterID_;
  uint64_t nodeID_;
  std::string cn_; // for output purpose, [clusterID_:nodeID_], e.g. [1:5]
  uint64_t leaderID_;
  uint64_t leaderTransferTargetID_;
  bool isLeaderTransferTarget_;
  bool pendingConfigChange_;
  State state_;
  uint64_t term_;
  uint64_t vote_;
  uint64_t applied_;
  std::unordered_map<uint64_t, bool> votes_;
  std::unordered_map<uint64_t, Remote> remotes_;
  std::unordered_map<uint64_t, Remote> observers_;
  std::unordered_map<uint64_t, Remote> witnesses_;
  std::vector<pbMessageUPtr> messages_;
  std::vector<uint64_t> matched_;
  LogEntrySPtr logEntry_;
  // ReadIndexSPtr readIndex_;
  std::vector<pbReadyToRead> readyToRead_;
  std::vector<pbEntry> droppedEntries_;
  std::vector<pbSystemCtx> droppedReadIndexes_;
  bool quiesce_;
  bool checkQuorum_;
  uint64_t tickCount_;
  uint64_t electionTick_;
  uint64_t heartbeatTick_;
  uint64_t electionTimeout_;
  uint64_t heartbeatTimeout_;
  uint64_t randomizedElectionTimeout_;
  uint64_t maxEntrySize_;
  uint64_t inMemoryGCTimeout_;
  std::mt19937_64 randomEngine_;
  using MessageHandler = void(Raft::*)(pbMessage &m);
  MessageHandler handlers_[NumOfState][NumOfMessageType];
  // FIXME
  // hasNotAppliedConfigChange
  server::RaftEventListenerSPtr listener_;
};
using RaftSPtr = std::shared_ptr<Raft>;
using RaftUPtr = std::shared_ptr<Raft>;

const char *StateToString(enum Raft::State state);

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_RAFT_H_
