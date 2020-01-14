//
// Created by jason on 2020/1/14.
//

#ifndef YCRT_RAFT_PEER_H_
#define YCRT_RAFT_PEER_H_

#include "utils/Utils.h"
#include "Raft.h"

namespace ycrt
{

namespace raft
{

class Peer {
 public:
  static std::unique_ptr<Peer> Launch();
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
  Peer();
  pbUpdate getUpdate(bool moreEntriesToApply, uint64_t lastApplied);
  void checkLaunchRequest();
  void bootstrap();
  slogger log;
  RaftUPtr raft_;
  pbState prevState_;
};
using PeerUPtr = std::unique_ptr<Peer>;

} // namespace raft

} // namespace ycrt



#endif //YCRT_RAFT_PEER_H_
