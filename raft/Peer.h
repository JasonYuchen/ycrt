//
// Created by jason on 2020/1/14.
//

#ifndef YCRT_RAFT_PEER_H_
#define YCRT_RAFT_PEER_H_

#include "utils/Utils.h"
#include "ycrt/Config.h"
#include "LogEntry.h"
#include "server/Event.h"
#include "Raft.h"

namespace ycrt
{

namespace raft
{

// Peer is the interface struct for interacting with the underlying Raft
// protocol implementation.
// Peer is the friend class of Raft
class Peer {
 public:
  DISALLOW_COPY_AND_ASSIGN(Peer);
  struct PeerInfo {
    uint64_t NodeID;
    std::string Address;
  };
  static std::unique_ptr<Peer> Launch(
    const Config &config,
    logdb::LogReaderSPtr logdb,
    server::RaftEventListenerSPtr listener,
    std::vector<PeerInfo> &peers,
    bool initial,
    bool newNode)
  {
    std::sort(peers.begin(), peers.end(),
      [](const PeerInfo &lhs, const PeerInfo &rhs) {
      return lhs.NodeID < rhs.NodeID;
    });
    checkLaunchRequest(config, peers, initial, newNode);
    std::unique_ptr<Peer> peer(new Peer(
      config,
      std::move(logdb),
      std::move(listener),
      peers,
      initial,
      newNode));
    return peer;
  }

  // Tick moves the logical clock forward by one tick.
  void Tick()
  {
    pbMessage m;
    m.set_type(raftpb::LocalTick);
    m.set_reject(false);
    raft_->Handle(m);
  }

  // QuiescedTick moves the logical clock forward by one tick in quiesced mode.
  void QuiescedTick()
  {
    pbMessage m;
    m.set_type(raftpb::LocalTick);
    m.set_reject(true);
    raft_->Handle(m);
  }

  // RequestLeaderTransfer makes a request to transfer the leadership to the
  // specified target node.
  void RequestLeaderTransfer(uint64_t target)
  {
    pbMessage m;
    m.set_type(raftpb::LeaderTransfer);
    m.set_to(raft_->nodeID_);
    m.set_from(target);
    m.set_hint(target);
    raft_->Handle(m);
  }

  // ProposeEntries proposes specified entries in a batched mode
  void ProposeEntries(std::vector<pbEntry> &&ents)
  {
    pbMessage m;
    m.set_type(raftpb::Propose);
    m.set_from(raft_->nodeID_);
    // FIXME: ents
    for (auto &ent : ents) {
      m.mutable_entries()->Add(std::move(ent));
    }
    raft_->Handle(m);
  }

  // ProposeConfigChange proposes a raft membership change.
  void ProposeConfigChange(const pbConfigChange &configChange, uint64_t key)
  {
    std::string config;
    if (!configChange.SerializeToString(&config)) {
      throw Error(ErrorCode::UnexpectedRaftMessage, "invalid config change");
    }
    pbMessage m;
    m.set_type(raftpb::Propose);
    pbEntry *entry = m.add_entries();
    entry->set_type(raftpb::ConfigChangeEntry);
    entry->set_cmd(std::move(config));
    entry->set_key(key);
    raft_->Handle(m);
  }

  // ReadIndex starts a ReadIndex operation. The ReadIndex protocol is defined in
  // the section 6.4 of the Raft thesis.
  void ReadIndex(pbReadIndexCtx ctx)
  {
    pbMessage m;
    m.set_type(raftpb::ReadIndex);
    m.set_hint(ctx.Low);
    m.set_hint_high(ctx.High);
    raft_->Handle(m);
  }

  // ApplyConfigChange applies a raft membership change to the local raft node.
  void ApplyConfigChange(const pbConfigChange &configChange)
  {
    if (configChange.node_id() == NoLeader) {
      raft_->pendingConfigChange_ = false;
      return;
    }
    pbMessage m;
    m.set_type(raftpb::ConfigChangeEvent);
    m.set_reject(false);
    m.set_hint(configChange.node_id());
    m.set_hint_high(configChange.type());
    raft_->Handle(m);
  }

  // RejectConfigChange rejects the currently pending raft membership change.
  void RejectConfigChange()
  {
    pbMessage m;
    m.set_type(raftpb::ConfigChangeEvent);
    m.set_reject(true);
    raft_->Handle(m);
  }

  // RestoreRemotes applies the remotes info obtained from the specified snapshot.
  void RestoreRemotes(pbSnapshotUPtr snapshot)
  {
    pbMessage m;
    m.set_type(raftpb::SnapshotReceived);
    // FIXME
    m.set_allocated_snapshot(snapshot.release());
    raft_->Handle(m);
  }

  // ReportUnreachableNode marks the specified node as not reachable.
  void ReportUnreachableNode(uint64_t nodeID)
  {
    pbMessage m;
    m.set_type(raftpb::Unreachable);
    m.set_from(nodeID);
    raft_->Handle(m);
  }

  // ReportSnapshotStatus reports the status of the snapshot to the local raft
  // node.
  void ReportSnapshotStatus(uint64_t nodeID, bool reject)
  {
    pbMessage m;
    m.set_type(raftpb::SnapshotStatus);
    m.set_from(nodeID);
    m.set_reject(reject);
    raft_->Handle(m);
  }

  // Handle processes the given message.
  void Handle(pbMessage &m)
  {
    if (Raft::isLocalMessage(m.type())) {
      throw Error(ErrorCode::UnexpectedRaftMessage, log,
        "Peer received local message");
    }
    auto r = raft_->remotes_.find(m.from()) != raft_->remotes_.end();
    auto o = raft_->observers_.find(m.from()) != raft_->observers_.end();
    auto w = raft_->witnesses_.find(m.from()) != raft_->witnesses_.end();
    if (r || o || w || !Raft::isResponseMessage(m.type())) {
      raft_->Handle(m);
    }
  }

  // GetUpdate returns the current state of the Peer.
  pbUpdate GetUpdate(bool moreToApply, uint64_t lastApplied) const
  {
    pbUpdate ud = getUpdate(moreToApply, lastApplied);
    validateUpdate(ud);
    setFastApply(ud);
    setUpdateCommit(ud);
    return ud;
  }

  // HasUpdate returns a boolean value indicating whether there is any Update
  // ready to be processed.
  bool HasUpdate(bool moreToApply) const
  {
    pbState curr = raft_->raftState();
    // FIXME: empty state?
    if (curr != pbState{} && curr != prevState_) {
      return true;
    }
    if (raft_->logEntry_->GetInMemorySnapshot() != nullptr) {
      return true;
    }
    if (!raft_->messages_->empty()) {
      return true;
    }
    if (!raft_->logEntry_->GetEntriesToSave().empty()) {
      return true;
    }
    if (moreToApply && raft_->logEntry_->HasEntriesToApply()) {
      return true;
    }
    if (!raft_->readyToRead_->empty()) {
      return true;
    }
    if (!raft_->droppedEntries_->empty() ||
      !raft_->droppedReadIndexes_->empty()) {
      return true;
    }
    return false;
  }

  // Commit commits the Update state to mark it as processed.
  void Commit(const pbUpdate &update)
  {
    raft_->messages_->clear();
    raft_->droppedEntries_->clear();
    raft_->droppedReadIndexes_->clear();
    if (update.State != pbState{}) {
      prevState_ = update.State;
    }
    if (update.UpdateCommit.ReadyToRead > 0) {
      raft_->readyToRead_->clear();
    }
    raft_->logEntry_->CommitUpdate(update.UpdateCommit);
  }

  bool HasEntryToApply() const
  {
    return raft_->logEntry_->HasEntriesToApply();
  }

 private:
  Peer(const Config &config,
    logdb::LogReaderSPtr logdb,
    server::RaftEventListenerSPtr listener,
    std::vector<PeerInfo> &peers,
    bool initial,
    bool newNode)
    : log(Log.GetLogger("raft")),
      raft_(),
      prevState_()
  {
    auto index = logdb->GetRange();
    raft_.reset(new Raft(config, std::move(logdb)));
    raft_->listener_ = std::move(listener);
    if (newNode && !config.IsObserver && !config.IsWitness) {
      raft_->becomeFollower(1, NoLeader);
    }
    if (initial && newNode) {
      bootstrap(peers);
    }
    if (index.second == 0) {
      prevState_ = pbState{}; // FIXME: Empty pbState
      prevState_.set_term(0);
      prevState_.set_vote(0);
      prevState_.set_commit(0);
    } else {
      prevState_ = raft_->raftState();
    }
    log->info("Peer {0} launched, initial={1}, newNode={2}",
      raft_->cn_, initial, newNode);
  }

  static void checkLaunchRequest(
    const Config &config,
    const std::vector<PeerInfo> &peers,
    bool initial,
    bool newNode)
  {
    if (config.NodeID == 0) {
      throw Error(ErrorCode::InvalidConfig, "NodeID must not be 0");
    }
    if (initial && newNode && peers.empty()) {
      throw Error(ErrorCode::InvalidConfig, "addresses must not be empty");
    }
    std::unordered_set<std::string> uniqueAddresses;
    for (auto &peer : peers) {
      uniqueAddresses.insert(peer.Address);
    }
    if (peers.size() != uniqueAddresses.size()) {
      throw Error(ErrorCode::InvalidConfig, "duplicated address found");
    }
  }

  void bootstrap(const std::vector<PeerInfo> &peers)
  {
    EntryVector ents;
    std::string config;
    ents.reserve(peers.size());
    for (auto &peer : peers) {
      log->info("{0}: inserting a bootstrap ConfigChange AddNode, "
                "Node={1}, Address={2}",
                raft_->describe(), peer.NodeID, peer.Address);
      pbConfigChange cc;
      cc.set_type(raftpb::AddNode);
      cc.set_node_id(peer.NodeID);
      cc.set_initialize(true);
      cc.set_address(peer.Address);
      config.clear();
      if (!cc.SerializeToString(&config)) {
        throw Error(ErrorCode::UnexpectedRaftMessage, "unexpected");
      }
      ents.emplace_back(std::make_shared<pbEntry>());
      ents.back()->set_type(raftpb::ConfigChangeEntry);
      ents.back()->set_term(1);
      ents.back()->set_index(ents.size());
      ents.back()->set_cmd(std::move(config));
    }
    raft_->logEntry_->Append({ents.data(), ents.size()});
    raft_->logEntry_->SetCommitted(ents.size());
    for (auto &peer : peers) {
      raft_->addNode(peer.NodeID);
    }
  }

  pbUpdate getUpdate(bool moreEntriesToApply, uint64_t lastApplied) const
  {
    pbUpdate ud;
    ud.ClusterID = raft_->clusterID_;
    ud.NodeID = raft_->nodeID_;
    ud.EntriesToSave = raft_->logEntry_->GetEntriesToSave();
    ud.Messages = raft_->messages_; // FIXME
    ud.LastApplied = lastApplied;
    ud.FastApply = true;
    if (moreEntriesToApply) {
      ud.CommittedEntries = raft_->logEntry_->GetEntriesToApply();
    }
    if (!ud.CommittedEntries.empty()) {
      ud.MoreCommittedEntries = raft_->logEntry_->HasMoreEntriesToApply(
        ud.CommittedEntries.back()->index());
    }
    ud.State = raft_->raftState();
    ud.Snapshot = raft_->logEntry_->GetInMemorySnapshot();
    ud.ReadyToReads = raft_->readyToRead_;
    ud.DroppedEntries = raft_->droppedEntries_;
    ud.DroppedReadIndexes = raft_->droppedReadIndexes_;
    return ud;
  }

  // TODO: move to the pbUpdate class
  void setFastApply(pbUpdate &ud) const
  {
    ud.FastApply = true;
    // if isEmptySnapshot
    if (!ud.Snapshot) {
      ud.FastApply = false;
    }
    if (ud.FastApply) {
      if (!ud.CommittedEntries.empty() && !ud.EntriesToSave.empty()) {
        uint64_t lastApply = ud.CommittedEntries.back()->index();
        uint64_t lastSave = ud.EntriesToSave.back()->index();
        uint64_t firstSave = ud.EntriesToSave.front()->index();
        if (lastApply >= firstSave && lastApply <= lastSave) {
          ud.FastApply = false;
        }
      }
    }
  }

  // TODO: move to the pbUpdate class
  void validateUpdate(const pbUpdate &ud) const
  {
    if (ud.State.commit() > 0 && !ud.CommittedEntries.empty()) {
      uint64_t lastIndex = ud.CommittedEntries.back()->index();
      if (lastIndex > ud.State.commit()) {
        throw Error(ErrorCode::InvalidUpdate, log,
          "try to apply not committed entry: commit={0} < last={1}",
          ud.State.commit(), lastIndex);
      }
    }
    if (!ud.CommittedEntries.empty() && !ud.EntriesToSave.empty()) {
      uint64_t lastApply = ud.CommittedEntries.back()->index();
      uint64_t lastSave = ud.EntriesToSave.back()->index();
      if (lastApply > lastSave) {
        throw Error(ErrorCode::InvalidUpdate, log,
          "try to apply not saved entry: save={0} < apply={1}",
          lastSave, lastApply);
      }
    }
  }

  // TODO: move to the pbUpdate class
  void setUpdateCommit(pbUpdate &ud) const
  {
    pbUpdateCommit &uc = ud.UpdateCommit;
    uc.ReadyToRead = ud.ReadyToReads->size();
    uc.LastApplied = ud.LastApplied;
    if (!ud.CommittedEntries.empty()) {
      uc.Processed = ud.CommittedEntries.back()->index();
    }
    if (!ud.EntriesToSave.empty()) {
      uc.StableLogIndex = ud.EntriesToSave.back()->index();
      uc.StableLogTerm = ud.EntriesToSave.back()->term();
    }
    if (!ud.Snapshot) {
      uc.StableSnapshotIndex = ud.Snapshot->index();
      uc.Processed = std::max(uc.Processed, uc.StableSnapshotIndex);
    }
  }

  slogger log;
  RaftUPtr raft_;
  pbState prevState_;
};
using PeerUPtr = std::unique_ptr<Peer>;

} // namespace raft

} // namespace ycrt



#endif //YCRT_RAFT_PEER_H_
