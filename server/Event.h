//
// Created by jason on 2020/1/9.
//

#ifndef YCRT_SERVER_EVENT_H_
#define YCRT_SERVER_EVENT_H_

#include <memory>
#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace server
{

struct LeaderInfo {
  NodeInfo Node;
  uint64_t Term = 0;
  uint64_t LeaderID = 0;
};

struct CampaignInfo {
  NodeInfo Node;
  uint64_t Term = 0;
};

struct SnapshotInfo {
  NodeInfo Node;
  uint64_t From = 0;
  uint64_t Index = 0;
  uint64_t Term = 0;
};

struct ReplicationInfo {
  NodeInfo Node;
  uint64_t From = 0;
  uint64_t Index = 0;
  uint64_t Term = 0;
};

struct ProposalInfo {
  NodeInfo Node;
  std::vector<pbEntry> Entries;
};

struct ReadIndexInfo {
  NodeInfo Node;
};

class RaftEventListener {
 public:
  void LeaderUpdated(const LeaderInfo &info) {}
  void CampaignLaunched(const CampaignInfo &info) {}
  void CampaignSkipped(const CampaignInfo &info) {}
  void SnapshotRejected(const SnapshotInfo &info) {}
  void ReplicationRejected(const ReplicationInfo &info) {}
  void ProposalDropped(const ProposalInfo &info) {}
  void ReadIndexDropped(const ReadIndexInfo &info) {}
};

using RaftEventListenerSPtr = std::shared_ptr<RaftEventListener>;

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_EVENT_H_
