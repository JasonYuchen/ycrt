//
// Created by jason on 2019/12/30.
//

#ifndef YCRT_PB_RAFTMESSAGE_H_
#define YCRT_PB_RAFTMESSAGE_H_

#include "raft.pb.h"

namespace ycrt
{

using MessageBatchSPtr = std::shared_ptr<raftpb::MessageBatch>;
using MessageBatchUPtr = std::unique_ptr<raftpb::MessageBatch>;
using MessageSPtr = std::shared_ptr<raftpb::Message>;
using MessageUPtr = std::unique_ptr<raftpb::Message>;
using SnapshotChunkSPtr = std::shared_ptr<raftpb::SnapshotChunk>;
using SnapshotChunkUPtr = std::unique_ptr<raftpb::SnapshotChunk>;
using SnapshotFileSPtr = std::shared_ptr<raftpb::SnapshotFile>;
using SnapshotFileUPtr = std::unique_ptr<raftpb::SnapshotFile>;
using StateSPtr = std::shared_ptr<raftpb::State>;
using StateUPtr = std::unique_ptr<raftpb::State>;
using SnapshotSPtr = std::shared_ptr<raftpb::Snapshot>;
using SnapshotUPtr = std::unique_ptr<raftpb::Snapshot>;
using MembershipSPtr = std::shared_ptr<raftpb::Membership>;
using MembershipUPtr = std::unique_ptr<raftpb::Membership>;
using EntrySPtr = std::shared_ptr<raftpb::Entry>;
using EntryUPtr = std::unique_ptr<raftpb::Entry>;

} // namespace ycrt

#endif //YCRT_PB_RAFTMESSAGE_H_
