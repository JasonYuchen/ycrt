//
// Created by jason on 2020/2/9.
//

#ifndef YCRT_LOGDB_LOGDB_H_
#define YCRT_LOGDB_LOGDB_H_

#include <future>
#include <memory>
#include <rocksdb/db.h>

#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace logdb
{

struct RaftState {
  pbState State;
  uint64_t FirstIndex;
  uint64_t EntryCount;
};

// TODO: actual persistent log manager (backed up by RocksDB)
// TODO: use Span<char> or shared_ptr<string> or string

// ReusableKey is the interface for keys that can be reused. A reusable key is
// usually obtained by calling the GetKey() function of the IContext
// instance.
class ReusableKey {
 public:
  void SetEntryBatchKey(NodeInfo node, uint64_t index);
  // SetEntryKey sets the key to be an entry key for the specified Raft node
  // with the specified entry index.
  void SetEntryKey(NodeInfo node, uint64_t index);
  // SetStateKey sets the key to be an persistent state key suitable
  // for the specified Raft cluster node.
  void SetStateKey(NodeInfo node);
  // SetMaxIndexKey sets the key to be the max possible index key for the
  // specified Raft cluster node.
  void SetMaxIndexKey(NodeInfo node);
  // Key returns the underlying byte slice of the key.
  Span<char> Key();
  // Release releases the key instance so it can be reused in the future.
  void Release();
 private:
};

// Context is the per thread context used in the logdb module.
// Context is expected to contain a list of reusable keys and byte
// slices that are owned per thread so they can be safely reused by the same
// thread when accessing LogDB.
class Context {
 public:
  // Destroy destroys the Context instance.
  void Destroy();
  // Reset resets the Context instance, all previous returned keys and
  // buffers will be put back to the IContext instance and be ready to
  // be used for the next iteration.
  void Reset();
  // GetKey returns a reusable key.
  ReusableKey GetKey();
  // GetValueBuffer returns a byte buffer with at least sz bytes in length.
  Span<char> GetValueBuffer(uint64_t size);
  // GetUpdates return a raftpb.Update slice,
  Span<pbUpdate> GetUpdates();
  // GetWriteBatch returns a write batch or transaction instance.
  void * GetWriteBatch(); // TODO rocksdb::Batch
  // GetEntryBatch returns an entry batch instance.
  pbEntryBatch GetEntryBatch(); // TODO: pbEntryBatch is just multiple entries, consider std::vector<pbEntry>
  // GetLastEntryBatch returns an entry batch instance.
  pbEntryBatch GetLastEntryBatch(); // TODO: pbEntryBatch is just multiple entries, consider std::vector<pbEntry>
 private:
};

// LogDB is the interface implemented by the log DB for persistently store
// Raft states, log entries and other Raft metadata.
class LogDB {
 public:
  static std::unique_ptr<LogDB> New();
  // Name returns the type name of the LogDB instance.
  const std::string &Name() const;
  // Close closes the LogDB instance.
  void Close();
  // BinaryFormat returns an constant uint32 value representing the binary
  // format version compatible with the LogDB instance.
  uint32_t BinaryFormat() const;
  // GetThreadContext returns a new Context instance.
  Context GetThreadContext();
  // ListNodeInfo lists all available NodeInfo found in the log DB.
  StatusWith<std::vector<NodeInfo>> ListNodeInfo();
  // SaveBootstrapInfo saves the specified bootstrap info to the log DB.
  Status SaveBootstrapInfo(NodeInfo node, const pbBootstrap bootstrap);
  // GetBootstrapInfo returns saved bootstrap info from log DB. It returns
  // ErrNoBootstrapInfo when there is no previously saved bootstrap info for
  // the specified node.
  StatusWith<pbBootstrap> GetBootstrapInfo(NodeInfo node);
  // SaveRaftState atomically saves the Raft states, log entries and snapshots
  // metadata found in the pb.Update list to the log DB.
  Status SaveRaftState(const Span<pbUpdate> updates, const Context &context);
  // GetRaftState returns the persistented raft state found in Log DB.
  StatusWith<RaftState> GetRaftState();
  // GetEntries returns the continuous Raft log entries of the specified
  // Raft node between the index value range of [low, high) up to a max size
  // limit of maxSize bytes. It append the located log entries to the argument
  // entries, returns their total size in bytes or an occurred error.
  // entries should be an empty vector by design
  StatusWith<uint64_t> GetEntries(
    EntryVector &entries,
    NodeInfo node,
    uint64_t low,
    uint64_t high,
    uint64_t maxSize)
  {
    // FIXME
    return ErrorCode::Other;
  };
  // RemoveEntriesTo removes entries associated with the specified Raft node up
  // to the specified index.
  Status RemoveEntriesTo(NodeInfo node, uint64_t index);
  // CompactEntriesTo reclaims underlying storage space used for storing
  // entries up to the specified index.
  StatusWith<std::future<void>> CompactEntriesTo(NodeInfo node, uint64_t index);
  // SaveSnapshots saves all snapshot metadata found in the pb.Update list.
  Status SaveSnapshot(const Span<pbUpdate> updates);
  // DeleteSnapshot removes the specified snapshot metadata from the log DB.
  Status DeleteSnapshot(NodeInfo node, uint64_t index);
  // ListSnapshots lists available snapshots associated with the specified
  // Raft node for index range (0, index].
  StatusWith<std::vector<pbSnapshotUPtr>> ListSnapshot(
    NodeInfo node,
    uint64_t index);
  // RemoveNodeData removes all data associated with the specified node.
  Status RemoveNodeData(NodeInfo node);
  // ImportSnapshot imports the specified snapshot by creating all required
  // metadata in the logdb.
  Status ImportSnapshot(uint64_t nodeID, pbSnapshotSPtr snapshot);

};
using LogDBSPtr = std::shared_ptr<LogDB>;

} // namespace logdb

} // namespace ycrt

#endif //YCRT_LOGDB_LOGDB_H_
