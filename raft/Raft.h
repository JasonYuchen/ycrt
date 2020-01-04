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

#include "utils/Utils.h"
#include "Remote.h"
#include "pb/RaftMessage.h"
#include "LogEntry.h"
#include "ycrt/Config.h"

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
  void Handle(pbMessageSPtr&);
 private:
  void initializeHandlerMap();
  void checkHandlerMapOrThrow();

  // status
  void loadState(const pbStateSPtr &state);
  bool isFollower();
  bool isPreCandidate();
  bool isCandidate();
  bool isLeader();
  bool isObserver();
  bool isWitness();
  void isLeaderOrThrow();
  uint64_t quorum();
  uint64_t numVotingMembers();
  void resetMatchValueArray();
  bool isSingleNodeQuorum();
  bool leaderHasQuorum();
  std::vector<uint64_t> getNodes();
  std::vector<uint64_t> getSortedNodes();
  std::unordered_map<uint64_t, Remote> getVotingMembers;

  // tick
  void tick();
  void leaderTick();
  void nonLeaderTick();
  void quiescedTick();
  bool timeForElection();
  bool timeForHeartbeat();
  bool timeForCheckQuorum();
  bool timeForAbortLeaderTransfer();
  void setRandomizedElectionTimeout();

  // send
  void finalizeMessageTerm(pbMessageSPtr &m);
  void send(pbMessageSPtr);
  void sendReplicateMessage(uint64_t to);
  void broadcastReplicateMessage();
  void sendHeartbeatMessage(uint64_t to /*, pbSystemCtx hint*/, uint64_t match);
  void broadcastHeartbeatMessage();
  void broadcastHeartbeatMessage(/*pbSystemCtx hint*/uint64_t hint);
  void sendTimeoutNowMessage(uint64_t to);

  // message generation
  pbMessageSPtr makeInstallSnapshotMessage(uint64_t to);
  pbMessageSPtr makeReplicateMessage(uint64_t to, uint64_t next, uint64_t maxSize);
  std::vector<pbEntrySPtr> makeMetadataEntries(std::vector<pbEntrySPtr> &entries);

  // message dropped
  void reportDroppedConfigChange(pbEntrySPtr);
  void reportDroppedProposal(pbMessageSPtr);
  void reportDroppedReadIndex(pbMessageSPtr);

  // log append and commit
  void sortMatchValues();
  bool tryCommit();
  void appendEntries(std::vector<pbEntrySPtr> &entries);

  // state transition
  void reset();
  void resetRemotes();
  void resetObservers();
  void resetWitnesses();
  void becomeObserver(uint64_t term, uint64_t leaderID);
  void becomeWitness(uint64_t term, uint64_t leaderID);
  void becomeFollower(uint64_t term, uint64_t leaderID);
  void becomePreCandidate();
  void becomeCandidate();
  void becomeLeader();

  // election
  void campaign();
  uint64_t handleVoteResp(uint64_t from, bool rejected);
  bool canGrantVote(pbMessageSPtr&);

  // membership
  bool selfRemoved();
  void addNode(uint64_t nodeID);
  void addObserver(uint64_t nodeID);
  void addWitness(uint64_t nodeID);
  void removeNode(uint64_t nodeID);
  void deleteRemote(uint64_t nodeID);
  void deleteObserver(uint64_t nodeID);
  void deleteWitness(uint64_t nodeID);
  void setRemote(uint64_t nodeID, uint64_t match, uint64_t next);
  void setObserver(uint64_t nodeID, uint64_t match, uint64_t next);
  void setWitness(uint64_t nodeID, uint64_t match, uint64_t next);
  bool hasPendingConfigChange();
  void setPendingConfigChange(bool isPendingConfigChange);
  uint64_t getPendingConfigChangeCount();
  bool hasConfigChangeToApply();

  // handlers
  void handleHeartbeat(pbMessageSPtr&);
  void handleInstallSnapshot(pbMessageSPtr&);
  void handleReplicate(pbMessageSPtr&);
  bool isRequestMessage(pbMessageType type);
  bool isLeaderMessage(pbMessageType type);
  bool dropRequestVoteFromHighTermNode(pbMessageSPtr&);
  bool onMessageTermNotMatched(pbMessageSPtr&);
  void handleElection(pbMessageSPtr&);
  void handleRequestVote(pbMessageSPtr&);
  void handleConfigChange(pbMessageSPtr&);
  void handleLocalTick(pbMessageSPtr&);
  void handleRestoreRemote(pbMessageSPtr&);

  // handlers in Leader node
  void handleLeaderPropose(pbMessageSPtr&);
  void handleLeaderHeartbeat(pbMessageSPtr&);
  void handleLeaderCheckQuorum(pbMessageSPtr&);
  bool hasCommittedEntryAtCurrentTerm();
  // addReadyToread
  void handleLeaderReadIndex(pbMessageSPtr&);
  void handleLeaderReplicateResp(pbMessageSPtr&);
  void handleLeaderHeartbeatResp(pbMessageSPtr&);
  void handleLeaderLeaderTransfer(pbMessageSPtr&);
  void handleReadIndexLeaderConfirmation(pbMessageSPtr&);
  void handleLeaderSnapshotStatus(pbMessageSPtr&);
  void handleLeaderUnreachable(pbMessageSPtr&);
  Remote *getRemoteByNodeID(uint64_t nodeID);

  // handlers in Observer - re-route to follower's handlers
  void handleObserverPropose(pbMessageSPtr&);
  void handleObserverReplicate(pbMessageSPtr&);
  void handleObserverHeartbeat(pbMessageSPtr&);
  void handleObserverInstallSnapshot(pbMessageSPtr&);
  void handleObserverReadIndex(pbMessageSPtr&);
  void handleObserverReadIndexResp(pbMessageSPtr&);

  // handlers in witness - re-route to follower's handlers
  void handleWitnessReplicate(pbMessageSPtr&);
  void handleWitnessHeartbeat(pbMessageSPtr&);
  void handleWitnessInstallSnapshot(pbMessageSPtr&);

  // handlers in follower
  void handleFollowerPropose(pbMessageSPtr&);
  void handleFollowerReplicate(pbMessageSPtr&);
  void handleFollowerHeartbeat(pbMessageSPtr&);
  void handleFollowerReadIndex(pbMessageSPtr&);
  void handleFollowerLeaderTransfer(pbMessageSPtr&);
  void handleFollowerReadIndexResp(pbMessageSPtr&);
  void handleFollowerInstallSnapshot(pbMessageSPtr&);
  void handleFollowerTimeoutNow(pbMessageSPtr&);

  // handlers in candidate
  void termMatchedOrThrow(uint64_t term);
  void handleCandidatePropose(pbMessageSPtr&);
  void handleCandidateReplicate(pbMessageSPtr&);
  void handleCandidateHeartbeat(pbMessageSPtr&);
  void handleCandidateReadIndex(pbMessageSPtr&);
  void handleCandidateInstallSnapshot(pbMessageSPtr&);
  void handleCandidateRequestVoteResp(pbMessageSPtr&);

  slogger log;
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
  std::unordered_map<uint64_t, bool> votes_;
  std::unordered_map<uint64_t, Remote> remotes_;
  std::unordered_map<uint64_t, Remote> observers_;
  std::unordered_map<uint64_t, Remote> witnesses_;
  std::vector<pbMessageSPtr> messages_;
  std::vector<uint64_t> matched_;
  LogEntrySPtr log_;
  // ReadIndexSPtr readIndex_;
  // std::vector<ReadyToReadSPtr> readyToRead_;
  std::vector<pbEntrySPtr> droppedEntries_;
  // pbSystemCtxSPtr droppedReadIndexes_;
  bool quiesce_;
  bool checkQuorum_;
  uint64_t tickCount_;
  uint64_t electionTick_;
  uint64_t heartbeatTick_;
  uint64_t electionTimeout_;
  uint64_t heartbeatTimeout_;
  uint64_t randomizedElectionTimeout_;
  using MessageHandler = void(Raft::*)(pbMessageSPtr&);
  MessageHandler handlers_[NumOfState][NumOfMessageType];
};

const char *StateToString(enum Raft::State state);

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_RAFT_H_
