//
// Created by jason on 2019/12/23.
//

#ifndef YCRT_SETTINGS_SOFT_H_
#define YCRT_SETTINGS_SOFT_H_

#include <stdint.h>

namespace ycrt
{

namespace settings
{

struct soft {
  /// Raft
  // MaxEntrySize defines the max total entry size that can be included in
  // the Replicate message.
  uint64_t MaxEntrySize = 64 * 1024 * 1024;
  // InMemEntrySliceSize defines the maximum length of the in memory entry
  // slice.
  uint64_t InMemEntrySliceSize = 512;
  // MinEntrySliceFreeSize defines the minimum length of the free in memory
  // entry slice. A new entry slice of length InMemEntrySliceSize will be
  // allocated once the free entry size in the current slice is less than
  // MinEntrySliceFreeSize.
  uint64_t MinEntrySliceFreeSize = 96;
  // InMemGCTimeout defines how often dragonboat collects partial object.
  // It is defined in terms of number of ticks.
  uint64_t InMemGCTimeout = 100;

  /// Multi-raft
  // SyncTaskInterval defines the interval in millisecond of periodic sync
  // state machine task.
  uint64_t SyncTaskInterval = 180000;
  // IncomingReadIndexQueueLength defines the number of pending read index
  // requests allowed for each raft group.
  uint64_t IncomingReadIndexQueueLength = 4096;
  // IncomingProposalQueueLength defines the number of pending proposals
  // allowed for each raft group.
  uint64_t IncomingProposalQueueLength = 2048;
  // ReceiveQueueLength is the length of the receive queue on each node.
  uint64_t ReceiveQueueLength = 1024;
  // SnapshotStatusPushDelayMS is the number of millisecond delays we impose
  // before pushing the snapshot results to raft node.
  uint64_t SnapshotStatusPushDelayMS = 1000;
  // TaskQueueTargetLength defined the target length of each node's taskQ.
  // Dragonboat tries to make sure the queue is no longer than this target
  // length.
  uint64_t TaskQueueTargetLength = 1024;
  // TaskQueueInitialCap defines the initial capcity of a task queue.
  uint64_t TaskQueueInitialCap = 64;
  // NodeHostSyncPoolSize defines the number of sync pools.
  uint64_t NodeHostSyncPoolSize = 8;
  // LatencySampleRatio defines the ratio how often latency is sampled.
  // It samples roughly every LatencySampleRatio ops.
  uint64_t LatencySampleRatio = 0;
  // LazyFreeCycle defines how often should entry queue and message queue
  // to be freed.
  uint64_t LazyFreeCycle = 1;
  // PanicOnSizeMismatch defines whether dragonboat should panic when snapshot
  // file size doesn't match the size recorded in snapshot metadata.
  bool PanicOnSizeMismatch = true;

  /// Replicated State Machine
  bool BatchedEntryApply = true;

  /// Step Engine
  // TaskBatchSize defines the length of the committed batch slice.
  uint64_t TaskBatchSize = 512;
  // NodeReloadMS defines how often step engine should reload
  // nodes, it is defined in number of millisecond.
  uint64_t NodeReloadMS = 200;
  // StepEngineTaskWorkerCount is the number of workers to use to apply
  // proposals (processing committed proposals) to application state
  // machines.
  uint64_t StepEngineTaskWorkerCount = 16;
  // StepEngineSnapshotWorkerCount is the number of workers to take and
  // apply application state machine snapshots.
  uint64_t StepEngineSnapshotWorkerCount = 64;

  /// Transport
  // GetConnectedTimeoutS is the default timeout value in second when
  // trying to connect to a gRPC based server.
  uint64_t GetConnectedTimeoutS = 5;
  // MaxSnapshotConnections defines the max number of concurrent outgoing
  // snapshot connections.
  uint64_t MaxSnapshotConnections = 64;
  // MaxConcurrentStreamingSnapshot defines the max number of concurrent
  // incoming snapshot streams.
  uint64_t MaxConcurrentStreamingSnapshot = 128;
  // SendQueueLength is the length of the send queue used to hold messages
  // exchanged between nodehosts. You may need to increase this value when
  // you want to host large number nodes per nodehost.
  uint64_t SendQueueLength = 2048;
  // StreamConnections defines how many connections to use for each remote
  // nodehost when exchanging raft messages
  uint64_t StreamConnections = 4;
  // PerConnectionSendBufSize is the size of the per connection buffer used for
  // sending messages.
  uint64_t PerConnectionSendBufSize = 64 * 1024 * 1024;
  // PerConnectionRecvBufSize is the size of the per connection buffer used for
  // receiving messages.
  uint64_t PerConnectionRecvBufSize = 2 * 1024 * 1024;
  // SnapshotGCTick defines the number of ticks between two snapshot GC
  // operations.
  uint64_t SnapshotGCTick = 30;
  // SnapshotChunkTimeoutTick defines the max time allowed to receive
  // a snapshot.
  uint64_t SnapshotChunkTimeoutTick = 900;

  /// LogDB
  uint64_t RocksDBKeepLogFileNum = 16;
  uint64_t RocksDBMaxBackgroundCompactions = 2;
  uint64_t RocksDBMaxBackgroundFlushes = 2;
  uint64_t RocksDBLRUCacheSize = 0;
  uint64_t RocksDBWriteBufferSize = 256 * 1024 * 1024;
  uint64_t RocksDBMaxWriteBufferNumber = 8;
  uint64_t RocksDBLevel0FileNumCompactionTrigger = 8;
  uint64_t RocksDBLevel0SlowdownWritesTrigger = 17;
  uint64_t RocksDBLevel0StopWritesTrigger = 24;
  uint64_t RocksDBMaxBytesForLevelBase = 4 * 1024 * 1024 * 1024L;
  uint64_t RocksDBMaxBytesForLevelMultiplier = 2;
  uint64_t RocksDBTargetFileSizeBase = 16 * 1024 * 1024;
  uint64_t RocksDBTargetFileSizeMultiplier = 2;
  uint64_t RocksDBLevelCompactionDynamicLevelBytes = 0;
  uint64_t RocksDBRecycleLogFileNum = 0;

  static soft &ins();
};

} // namespace settings

} // namespace ycrt

#endif //YCRT_SETTINGS_SOFT_H_
