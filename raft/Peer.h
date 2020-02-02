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

class Peer {
 public:
  struct PeerInfo {
    uint64_t NodeID;
    std::string Address;
  };
  static std::unique_ptr<Peer> Launch(
    const Config &config,
    LogReaderSPtr logdb,
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

  void Tick()
  {
    pbMessage m;
    m.set_type(raftpb::LocalTick);
    m.set_reject(false);
    raft_->Handle(m);
  }

  void QuiescedTick()
  {
    pbMessage m;
    m.set_type(raftpb::LocalTick);
    m.set_reject(true);
    raft_->Handle(m);
  }

  void RequestLeaderTransfer(uint64_t target)
  {
    pbMessage m;
    m.set_type(raftpb::LeaderTransfer);
    m.set_to(raft_->nodeID_);
    m.set_from(target);
    m.set_hint(target);
    raft_->Handle(m);
  }

  void ProposeEntries(std::vector<pbEntry> ents)
  {
    pbMessage m;
    m.set_type(raftpb::Propose);
    m.set_from(raft_->nodeID_);
    // FIXME: ents
    raft_->Handle(m);
  }

  void ProposeConfigChange(pbConfigChange configChange, uint64_t key)
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

  void ApplyConfigChange(pbConfigChange configChange);
  void RejectConfigChange();
  void RestoreRemotes(pbSnapshot snapshot);
  void ReportUnreachableNode(uint64_t nodeID);
  void ReportSnapshotStatus(uint64_t nodeID, bool reject);
  void Handle(pbMessage &m);
  pbUpdate GetUpdate(bool moreToApply, uint64_t lastApplied);
  bool HasUpdate(bool moreToApply);
  void Commit(const pbUpdate &update);
  void ReadIndex(pbSystemCtx ctx);
  bool HasEntryToApply();
 private:
  Peer(const Config &config,
    LogReaderSPtr logdb,
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
      prevState_ = pbState{};
    } else {
      prevState_ = raft_->raftState();
    }
    log->info("Peer {0} launched, initial={1}, newNode={2}", raft_->cn_, initial, newNode);
  }
  pbUpdate getUpdate(bool moreEntriesToApply, uint64_t lastApplied)
  {
    auto ud = pbUpdate{};
    if (moreEntriesToApply) {
      ud.CommittedEntries = raft_->logEntry_->GetEntriesToApply();
    }
    if (!ud.CommittedEntries.empty()) {
      ud.MoreCommittedEntries = raft_->logEntry_->HasMoreEntriesToApply(
        ud.CommittedEntries.back().index());
    }
    pbState presentState = raft_->raftState();
    if (presentState != prevState_) {
      ud.State = presentState;
    }
    // FIXME: inmem snapshot
    if (raft_->logEntry_);
    if (!raft_->readyToRead_.empty()) {
      ud.ReadyToReads = raft_->readyToRead_;
    }
    if (!raft_->droppedEntries_.empty()) {
      ud.DroppedEntries = raft_->droppedEntries_;
    }
    if (!raft_->droppedReadIndexes_.empty()) {
      ud.DroppedReadIndexes = raft_->droppedReadIndexes_;
    }
    return ud;
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
    std::vector<pbEntry> ents;
    std::string config;
    ents.reserve(peers.size());
    for (auto &peer : peers) {
      log->info("{0}: inserting a bootstrap ConfigChange AddNode, Node={1}, Address={2}", raft_->describe(), peer.NodeID, peer.Address);
      pbConfigChange cc;
      cc.set_type(raftpb::AddNode);
      cc.set_node_id(peer.NodeID);
      cc.set_initialize(true);
      cc.set_address(peer.Address);
      config.clear();
      if (!cc.SerializeToString(&config)) {
        throw Error(ErrorCode::UnexpectedRaftMessage, "unexpected");
      }
      ents.emplace_back(pbEntry{});
      ents.back().set_type(raftpb::ConfigChangeEntry);
      ents.back().set_term(1);
      ents.back().set_index(ents.size());
      ents.back().set_cmd(std::move(config));
    }
    raft_->logEntry_->Append({ents.data(), ents.size()});
    raft_->logEntry_->SetCommitted(ents.size());
    for (auto &peer : peers) {
      raft_->addNode(peer.NodeID);
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
