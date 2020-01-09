//
// Created by jason on 2020/1/9.
//

#ifndef YCRT_SERVER_EVENT_H_
#define YCRT_SERVER_EVENT_H_

#include <memory>
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace server
{

struct LeaderInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  uint64_t Term;
  uint64_t LeaderID;
};

struct CampaignInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  bool PreVote;
  uint64_t Term;
};

struct SnapshotInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  uint64_t From;
  uint64_t Index;
  uint64_t Term;
};

struct ReplicationInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  uint64_t From;
  uint64_t Index;
  uint64_t Term;
};

struct ProposalInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  std::vector<pbEntry> Entries;
};

struct ReadIndexInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
};

class RaftEventListener {
 public:
  void LeaderUpdated(const LeaderInfo &info);
  void CampaignLaunched(const CampaignInfo &info);
  void CampaignSkipped(const CampaignInfo &info);
  void SnapshotRejected(const SnapshotInfo &info);
  void ReplicationRejected(const ReplicationInfo &info);
  void ProposalDropped(const ProposalInfo &info);
  void ReadIndexDropped(const ReadIndexInfo &info);
};

using RaftEventListenerSPtr = std::shared_ptr<RaftEventListener>;

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_EVENT_H_
