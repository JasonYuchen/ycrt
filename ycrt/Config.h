//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_YCRT_CONFIG_H_
#define YCRT_YCRT_CONFIG_H_

#include <stdint.h>
#include "pb/raft.pb.h"

namespace ycrt
{

enum CompressionType {
  NoCompression = raftpb::NoCompression,
  Snappy = raftpb::Snappy,
};

struct Config {
  // NodeID is a non-zero value used to identify a node within a Raft cluster.
  uint64_t NodeID;
  // ClusterID is the unique value used to identify a Raft cluster.
  uint64_t ClusterID;
  // CheckQuorum specifies whether the leader node should periodically check
  // non-leader node status and step down to become a follower node when it no
  // longer has the quorum.
  bool CheckQuorum;
  // ElectionRTT is the minimum number of message RTT between elections. Message
  // RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggests it
  // to be a magnitude greater than HeartbeatRTT, which is the interval between
  // two heartbeats. In Raft, the actual interval between elections is
  // randomized to be between ElectionRTT and 2 * ElectionRTT.
  //
  // As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond,
  // to set the election interval to be 1 second, then ElectionRTT should be set
  // to 10.
  //
  // When CheckQuorum is enabled, ElectionRTT also defines the interval for
  // checking leader quorum.
  uint64_t ElectionRTT;
  // HeartbeatRTT is the number of message RTT between heartbeats. Message
  // RTT is defined by NodeHostConfig.RTTMillisecond. The Raft paper suggest the
  // heartbeat interval to be close to the average RTT between nodes.
  //
  // As an example, assuming NodeHostConfig.RTTMillisecond is 100 millisecond,
  // to set the heartbeat interval to be every 200 milliseconds, then
  // HeartbeatRTT should be set to 2.
  uint64_t HeartbeatRTT;
  // SnapshotEntries defines how often the state machine should be snapshotted
  // automcatically. It is defined in terms of the number of applied Raft log
  // entries. SnapshotEntries can be set to 0 to disable such automatic
  // snapshotting.
  //
  // When SnapshotEntries is set to N, it means a snapshot is created for
  // roughly every N applied Raft log entries (proposals). This also implies
  // that sending N log entries to a follower is more expensive than sending a
  // snapshot.
  //
  // Once a snapshot is generated, Raft log entries covered by the new snapshot
  // can be compacted. See the godoc on CompactionOverhead to see what log
  // entries are actually compacted after taking a snapshot.
  //
  // NodeHost.RequestSnapshot can be called to manually request a snapshot to
  // be created to capture current node state.
  //
  // Once automatic snapshotting is disabled by setting the SnapshotEntries
  // field to 0, users can still use NodeHost's RequestSnapshot or
  // SyncRequestSnapshot methods to manually request snapshots.
  uint64_t SnapshotEntries;
  // CompactionOverhead defines the number of most recent entries to keep after
  // each Raft log compaction. Raft log compaction is performance automatically
  // every time when a snapshot is created.
  //
  // For example, when a snapshot is created at let's say index 10,000, then all
  // Raft log entries with index <= 10,000 can be removed from that node as they
  // have already been covered by the created snapshot image. This frees up the
  // maximum storage space but comes at the cost that the full snapshot will
  // have to be sent to the follower if the follower requires any Raft log entry
  // at index <= 10,000. When CompactionOverhead is set to say 500, Dragonboat
  // then compacts the Raft log up to index 9,500 and keeps Raft log entries
  // between index (9,500, 1,0000]. As a result, the node can still replicate
  // Raft log entries between index (9,500, 1,0000] to other peers and only fall
  // back to stream the full snapshot if any Raft log entry with index <= 9,500
  // is required to be replicated.
  uint64_t CompactionOverhead;
  // OrderedConfigChange determines whether Raft membership change is enforced
  // with ordered config change ID.
  bool OrderedConfigChange;
  // MaxInMemLogSize is the target size in bytes allowed for storing in memory
  // Raft logs on each Raft node. In memory Raft logs are the ones that have
  // not been applied yet.
  // MaxInMemLogSize is a target value implemented to prevent unbounded memory
  // growth, it is not for precisely limiting the exact memory usage.
  // When MaxInMemLogSize is 0, the target is set to math.MaxUint64. When
  // MaxInMemLogSize is set and the target is reached, error will be returned
  // when clients try to make new proposals.
  // MaxInMemLogSize is recommended to be significantly larger than the biggest
  // proposal you are going to use.
  uint64_t MaxInMemLogSize;
  // SnapshotCompressionType is the compression type to use for compressing
  // generated snapshot data. No compression is used by default.
  CompressionType SnapshotCompressionType;
  // EntryCompressionType is the compression type to use for compressing the
  // payload of user proposals. When Snappy is used, the maximum proposal
  // payload allowed is roughly limited to 3.42GBytes.
  CompressionType EntryCompressionType;
  // IsObserver indicates whether this is an observer Raft node without voting
  // power. Described as non-voting members in the section 4.2.1 of Diego
  // Ongaro's thesis, observer nodes are usually used to allow a new node to
  // join the cluster and catch up with other existing ndoes without impacting
  // the availability. Extra observer nodes can also be introduced to serve
  // read-only requests without affecting system write throughput.
  //
  // Observer support is currently experimental.
  bool IsObserver;
  // IsWitness indicates whether this is a witness Raft node without actual log
  // replication and do not have state machine. It is mentioned in the section
  // 11.7.2 of Diego Ongaro's thesis.
  //
  // Witness support is currently experimental.
  bool IsWitness;
  // Quiesce specifies whether to let the Raft cluster enter quiesce mode when
  // there is no cluster activity. Clusters in quiesce mode do not exchange
  // heartbeat messages to minimize bandwidth consumption.
  //
  // Quiesce support is currently experimental.
  bool Quiesce;

  void validate();
};

struct NodeHostConfig {

};

} // namespace ycrt

#endif //YCRT_YCRT_CONFIG_H_
