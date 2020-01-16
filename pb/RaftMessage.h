//
// Created by jason on 2019/12/30.
//

#ifndef YCRT_PB_RAFTMESSAGE_H_
#define YCRT_PB_RAFTMESSAGE_H_

#include "raft.pb.h"
#include <vector>

namespace ycrt
{
// TODO: use hand-written serialization
using pbMessageType = raftpb::MessageType;
using pbMessageBatch = raftpb::MessageBatch;
using pbMessageBatchSPtr = std::shared_ptr<raftpb::MessageBatch>;
using pbMessageBatchUPtr = std::unique_ptr<raftpb::MessageBatch>;
using pbMessage = raftpb::Message;
using pbMessageSPtr = std::shared_ptr<raftpb::Message>;
using pbMessageUPtr = std::unique_ptr<raftpb::Message>;
using pbSnapshotChunk = raftpb::SnapshotChunk;
using pbSnapshotChunkSPtr = std::shared_ptr<raftpb::SnapshotChunk>;
using pbSnapshotChunkUPtr = std::unique_ptr<raftpb::SnapshotChunk>;
using pbSnapshotFile = raftpb::SnapshotFile;
using pbSnapshotFileSPtr = std::shared_ptr<raftpb::SnapshotFile>;
using pbSnapshotFileUPtr = std::unique_ptr<raftpb::SnapshotFile>;
using pbState = raftpb::State;
using pbStateSPtr = std::shared_ptr<raftpb::State>;
using pbStateUPtr = std::unique_ptr<raftpb::State>;
using pbSnapshot = raftpb::Snapshot;
using pbSnapshotSPtr = std::shared_ptr<raftpb::Snapshot>;
using pbSnapshotUPtr = std::unique_ptr<raftpb::Snapshot>;
using pbMembership = raftpb::Membership;
using pbMembershipSPtr = std::shared_ptr<raftpb::Membership>;
using pbMembershipUPtr = std::unique_ptr<raftpb::Membership>;
using pbEntry = raftpb::Entry;
using pbEntrySPtr = std::shared_ptr<raftpb::Entry>;
using pbEntryUPtr = std::unique_ptr<raftpb::Entry>;
using pbConfigChange = raftpb::ConfigChange;
using pbConfigChangeSPtr = std::shared_ptr<raftpb::ConfigChange>;
using pbConfigChangeUPtr = std::unique_ptr<raftpb::ConfigChange>;

typedef struct SystemCtx {
  uint64_t Low;
  uint64_t High;
} pbSystemCtx;

struct SystemCtxHash {
  size_t operator()(const SystemCtx& rhs) const {
    return std::hash<uint64_t>()(rhs.Low)
      ^ std::hash<uint64_t>()(rhs.High);
  }
};

inline bool operator==(const SystemCtx &lhs, const SystemCtx &rhs)
{
  return lhs.Low == rhs.Low && lhs.High == rhs.High;
}

inline bool operator==(const pbState &lhs, const pbState &rhs)
{
  return
    lhs.term() == rhs.term() &&
    lhs.vote() == rhs.vote() &&
    lhs.commit() == rhs.commit();
}

inline bool operator!=(const pbState &lhs, const pbState &rhs)
{
  return !(lhs == rhs);
}

typedef struct ReadyToRead {
  uint64_t Index;
  struct SystemCtx SystemCtx;
} pbReadyToRead;

constexpr uint8_t NumOfMessageType = 28;

typedef struct UpdateCommit {
  uint64_t Processed;
  uint64_t LastApplied;
  uint64_t StableLogTo;
  uint64_t StableLogTerm;
  uint64_t StableSnapshotTo;
  uint64_t ReadyToRead;
} pbUpdateCommit;

typedef struct Update {
  uint64_t ClusterID;
  uint64_t NodeID;
  // The current persistent state of a raft node. It must be stored onto
  // persistent storage before any non-replication can be sent to other nodes.
  // isStateEqual(emptyState) returns true when the state is empty.
  pbState State;
  // whether CommittedEntries can be applied without waiting for the Update
  // to be persisted to disk
  bool FastApply;
  // EntriesToSave are entries waiting to be stored onto persistent storage.
  std::vector<pbEntry> EntriesToSave;
  // CommittedEntries are entries already committed in raft and ready to be
  // applied by dragonboat applications.
  std::vector<pbEntry> CommittedEntries;
  // Whether there are more committed entries ready to be applied.
  bool MoreCommittedEntries;
  // Snapshot is the metadata of the snapshot ready to be applied.
  pbSnapshotUPtr Snapshot;
  // ReadyToReads provides a list of ReadIndex requests ready for local read.
  std::vector<pbReadyToRead> ReadyToReads;
  // Messages is a list of outgoing messages to be sent to remote nodes.
  // As stated above, replication messages can be immediately sent, all other
  // messages must be sent after the persistent state and entries are saved
  // onto persistent storage.
  std::vector<pbMessage> Messages;
  // LastApplied is the actual last applied index reported by the RSM.
  uint64_t LastApplied;
  // UpdateCommit contains info on how the Update instance can be committed
  // to actually progress the state of raft.
  pbUpdateCommit UpdateCommit;
  // DroppedEntries is a list of entries dropped when no leader is available
  std::vector<pbEntry> DroppedEntries;
  // DroppedReadIndexes is a list of read index requests  dropped when no leader
  // is available.
  std::vector<pbSystemCtx> DroppedReadIndexes;
} pbUpdate;

} // namespace ycrt

#endif //YCRT_PB_RAFTMESSAGE_H_
