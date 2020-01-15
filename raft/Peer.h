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
  static std::unique_ptr<Peer> Launch(
    const Config &config,
    LogDBUPtr logdb,
    server::RaftEventListenerSPtr listener,
    const std::vector<std::string> &peerAddresses,
    bool initial,
    bool newNode)
  {
    checkLaunchRequest(config, peerAddresses, initial, newNode);
    std::unique_ptr<Peer> peer(new Peer(
      std::move(config),
      std::move(logdb),
      std::move(listener),
      std::move(peerAddresses),
      initial,
      newNode));
    return peer;
  }
  void Tick();
  void QuiescedTick();
  void RequestLeaderTransfer(uint64_t target);
  void ProposeEntries(std::vector<pbEntry> ents);
  void ProposeConfigChange(pbConfigChange configChange, uint64_t key);
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
    LogDBUPtr logdb,
    server::RaftEventListenerSPtr listener,
    const std::vector<std::string> &peerAddresses,
    bool initial,
    bool newNode)
    : log(Log.GetLogger("raft")),
      raft_(),
      prevState_()
  {
    auto index = logdb->Range();
    raft_.reset(new Raft(config, std::move(logdb)));
    raft_->listener_ = std::move(listener);
    if (newNode && !config.IsObserver && !config.IsWitness) {
      raft_->becomeFollower(1, NoLeader);
    }
    if (initial && newNode) {
      bootstrap();
    }
    if (index.second == 0) {
      prevState_ = pbState{};
    } else {
      prevState_ = raft_->raftState();
    }
    log->info("Peer {0} launched, initial={1}, newNode={2}", raft_->cn_, initial, newNode);
  }
  pbUpdate getUpdate(bool moreEntriesToApply, uint64_t lastApplied);
  static void checkLaunchRequest(const Config &, const std::vector<std::string> &, bool, bool);
  void bootstrap();
  slogger log;
  RaftUPtr raft_;
  pbState prevState_;
};
using PeerUPtr = std::unique_ptr<Peer>;

} // namespace raft

} // namespace ycrt



#endif //YCRT_RAFT_PEER_H_
