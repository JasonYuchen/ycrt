//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_YCRT_CONFIG_H_
#define YCRT_YCRT_CONFIG_H_

#include <stdint.h>
#include <functional>
#include "pb/raft.pb.h"

namespace ycrt
{

enum CompressionType {
  NoCompression = raftpb::NoCompression,
  Snappy = raftpb::Snappy,
};

constexpr uint64_t NoLeader = 0;
constexpr uint64_t NoNode = 0;
constexpr uint64_t NoLimit = UINT64_MAX;

struct LeaderInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
  uint64_t Term;
  uint64_t LeaderID;
};

struct NodeInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
};

inline bool operator==(const NodeInfo &lhs, const NodeInfo &rhs)
{
  return lhs.NodeID == rhs.NodeID && lhs.ClusterID == rhs.ClusterID;
}

struct NodeInfoHash {
  size_t operator()(const NodeInfo& rhs) const {
    return std::hash<uint64_t>()(rhs.ClusterID)
      ^ std::hash<uint64_t>()(rhs.NodeID);
  }
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

  void Validate();
};
using ConfigSPtr = std::shared_ptr<Config>;

struct NodeHostConfig {
// DeploymentID is used to determine whether two NodeHost instances belong to
  // the same deployment and thus allowed to communicate with each other. This
  // helps to prvent accidentially misconfigured NodeHost instances to cause
  // data corruption errors by sending out of context messages to unrelated
  // Raft nodes.
  // For a particular dragonboat based application, you can set DeploymentID
  // to the same uint64 value on all production NodeHost instances, then use
  // different DeploymentID values on your staging and dev environment. It is
  // also recommended to use different DeploymentID values for different
  // dragonboat based applications.
  // When not set, the default value 0 will be used as the deployment ID and
  // thus allowing all NodeHost instances with deployment ID 0 to communicate
  // with each other.
  uint64_t DeploymentID;
  // WALDir is the directory used for storing the WAL of Raft entries. It is
  // recommended to use low latency storage such as NVME SSD with power loss
  // protection to store such WAL data. Leave WALDir to have zero value will
  // have everything stored in NodeHostDir.
  std::string WALDir;
  // NodeHostDir is where everything else is stored.
  std::string NodeHostDir;
  // RTTMillisecond defines the average Rround Trip Time (RTT) in milliseconds
  // between two NodeHost instances. Such a RTT interval is internally used as
  // a logical clock tick, Raft heartbeat and election intervals are both
  // defined in term of how many such RTT intervals.
  // Note that RTTMillisecond is the combined delays between two NodeHost
  // instances including all delays caused by network transmission, delays
  // caused by NodeHost queuing and processing. As an example, when fully
  // loaded, the average Rround Trip Time between two of our NodeHost instances
  // used for benchmarking purposes is up to 500 microseconds when the ping time
  // between them is 100 microseconds. Set RTTMillisecond to 1 when it is less
  // than 1 million in your environment.
  uint64_t RTTMillisecond;
  // RaftAddress is a hostname:port or IP:port address used by the Raft RPC
  // module for exchanging Raft messages and snapshots. This is also the
  // identifier for a NodeHost instance. RaftAddress should be set to the
  // public address that can be accessed from remote NodeHost instances.
  std::string RaftAddress;
  // ListenAddress is a hostname:port or IP:port address used by the Raft RPC
  // module to listen on for Raft message and snapshots. When the ListenAddress
  // field is not set, The Raft RPC module listens on RaftAddress. If 0.0.0.0
  // is specified as the IP of the ListenAddress, Dragonboat listens to the
  // specified port on all interfaces. When hostname or domain name is
  // specified, it is locally resolved to IP addresses first and Dragonboat
  // listens to all resolved IP addresses.
  std::string ListenAddress;
  // MutualTLS defines whether to use mutual TLS for authenticating servers
  // and clients. Insecure communication is used when MutualTLS is set to
  // False.
  // See https://github.com/lni/dragonboat/wiki/TLS-in-Dragonboat for more
  // details on how to use Mutual TLS.
  bool MutualTLS;
  // CAFile is the path of the CA certificate file. This field is ignored when
  // MutualTLS is false.
  std::string CAFile;
  // CertFile is the path of the node certificate file. This field is ignored
  // when MutualTLS is false.
  std::string CertFile;
  // KeyFile is the path of the node key file. This field is ignored when
  // MutualTLS is false.
  std::string KeyFile;
  // MaxSendQueueSize is the maximum size in bytes of each send queue.
  // Once the maximum size is reached, further replication messages will be
  // dropped to restrict memory usage. When set to 0, it means the send queue
  // size is unlimited.
  uint64_t MaxSendQueueSize;
  // MaxReceiveQueueSize is the maximum size in bytes of each receive queue.
  // Once the maximum size is reached, further replication messages will be
  // dropped to restrict memory usage. When set to 0, it means the queue size
  // is unlimited.
  uint64_t MaxReceiveQueueSize;
  // LogDBFactory is the factory function used for creating the Log DB instance
  // used by NodeHost. The default zero value causes the default built-in RocksDB
  // based Log DB implementation to be used.
  // FIXME: LogDBFactory LogDBFactoryFunc
  // RaftRPCFactory is the factory function used for creating the Raft RPC
  // instance for exchanging Raft message between NodeHost instances. The default
  // zero value causes the built-in TCP based RPC module to be used.
  // FIXME: RaftRPCFactory RaftRPCFactoryFunc
  // EnableMetrics determines whether health metrics in Prometheus format should
  // be enabled.
  bool EnableMetrics;
  // RaftEventListener is the listener for Raft events exposed to user space.
  // NodeHost uses a single dedicated goroutine to invoke all RaftEventListener
  // methods one by one, CPU intensive or IO related procedures that can cause
  // long delays should be offloaded to worker goroutines managed by users.
  std::function<void(LeaderInfo)> RaftEventListener;
  // MaxSnapshotSendBytesPerSecond defines how much snapshot data can be sent
  // every second for all Raft clusters managed by the NodeHost instance.
  // The default value 0 means there is no limit set for snapshot streaming.
  uint64_t MaxSnapshotSendBytesPerSecond;
  // MaxSnapshotRecvBytesPerSecond defines how much snapshot data can be
  // received each second for all Raft clusters managed by the NodeHost instance.
  // The default value 0 means there is no limit for receiving snapshot data.
  uint64_t MaxSnapshotRecvBytesPerSecond;

  void Validate();
};
using NodeHostConfigSPtr = std::shared_ptr<NodeHostConfig>;

} // namespace ycrt

#endif //YCRT_YCRT_CONFIG_H_
