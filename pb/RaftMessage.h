//
// Created by jason on 2019/12/30.
//

#ifndef YCRT_PB_RAFTMESSAGE_H_
#define YCRT_PB_RAFTMESSAGE_H_

#include "raft.pb.h"

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

typedef struct ReadyToRead {
  uint64_t Index;
  struct SystemCtx SystemCtx;
} pbReadyToRead;

constexpr uint8_t NumOfMessageType = 28;

} // namespace ycrt

#endif //YCRT_PB_RAFTMESSAGE_H_
