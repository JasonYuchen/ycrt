//
// Created by jason on 2020/2/1.
//

#ifndef YCRT_RAFT_LOGREADER_H_
#define YCRT_RAFT_LOGREADER_H_

#include <stdint.h>
#include <vector>
#include <memory>
#include <future>
#include <mutex>
#include <settings/Soft.h>
#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace logdb
{

struct NodeInfo {
  uint64_t ClusterID;
  uint64_t NodeID;
};

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
  void SetEntryBatchKey(uint64_t clusterID, uint64_t nodeID, uint64_t index);
  // SetEntryKey sets the key to be an entry key for the specified Raft node
  // with the specified entry index.
  void SetEntryKey(uint64_t clusterID, uint64_t nodeID, uint64_t index);
  // SetStateKey sets the key to be an persistent state key suitable
  // for the specified Raft cluster node.
  void SetStateKey(uint64_t clusterID, uint64_t nodeID);
  // SetMaxIndexKey sets the key to be the max possible index key for the
  // specified Raft cluster node.
  void SetMaxIndexKey(uint64_t clusterID, uint64_t nodeID);
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
  Status SaveBootstrapInfo(
    uint64_t clusterID,
    uint64_t nodeID,
    const pbBootstrap bootstrap);
  // GetBootstrapInfo returns saved bootstrap info from log DB. It returns
  // ErrNoBootstrapInfo when there is no previously saved bootstrap info for
  // the specified node.
  StatusWith<pbBootstrap> GetBootstrapInfo(uint64_t clusterID, uint64_t nodeID);
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
    uint64_t clusterID,
    uint64_t nodeID,
    uint64_t low,
    uint64_t high,
    uint64_t maxSize){};
  // RemoveEntriesTo removes entries associated with the specified Raft node up
  // to the specified index.
  Status RemoveEntriesTo(uint64_t clusterID, uint64_t nodeID, uint64_t index);
  // CompactEntriesTo reclaims underlying storage space used for storing
  // entries up to the specified index.
  StatusWith<std::future<void>> CompactEntriesTo(uint64_t clusterID, uint64_t nodeID, uint64_t index);
  // SaveSnapshots saves all snapshot metadata found in the pb.Update list.
  Status SaveSnapshot(const Span<pbUpdate> updates);
  // DeleteSnapshot removes the specified snapshot metadata from the log DB.
  Status DeleteSnapshot(uint64_t clusterID, uint64_t nodeID, uint64_t index);
  // ListSnapshots lists available snapshots associated with the specified
  // Raft node for index range (0, index].
  StatusWith<std::vector<pbSnapshotUPtr>> ListSnapshot(
    uint64_t clusterID,
    uint64_t nodeID,
    uint64_t index);
  // RemoveNodeData removes all data associated with the specified node.
  Status RemoveNodeData(uint64_t clusterID, uint64_t nodeID);
  // ImportSnapshot imports the specified snapshot by creating all required
  // metadata in the logdb.
  Status ImportSnapshot(uint64_t nodeID, pbSnapshotSPtr snapshot);

};
using LogDBSPtr = std::shared_ptr<LogDB>;

// LogReader is a read-only interface to the underlying persistent storage to
// allow the raft package to access raft state, entries, snapshots stored in
// the persistent storage. Entries stored in the persistent storage accessible
// via LogReader is usually not required in normal cases.
class LogReader {
 public:
  static std::unique_ptr<LogReader> New(uint64_t clusterID, uint64_t nodeID, LogDBSPtr logdb)
  {
    std::unique_ptr<LogReader> reader(new LogReader(clusterID, nodeID, std::move(logdb)));
    return reader;
  }
  DISALLOW_COPY_AND_ASSIGN(LogReader);

  // GetRange returns the range of the entries in LogReader.
  std::pair<uint64_t, uint64_t> GetRange()
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return {firstIndex(), lastIndex()};
  }

  // SetRange updates the LogReader to reflect what is available in it.
  void SetRange(uint64_t index, uint64_t length)
  {
    if (length == 0) {
      return;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    uint64_t curfirst = firstIndex();
    uint64_t setlast = index + length - 1;
    // current range includes the [index, index+length)
    if (setlast < curfirst) {
      return;
    }
    // overlap
    if (curfirst > index) {
      length -= curfirst - index;
      index = curfirst;
    }
    uint64_t offset = index - markerIndex_;
    if (length_ > offset) {
      length_ = offset + length;
    } else if (length_ == offset) {
      length_ += length;
    } else {
      // curfirst < index, log hole found
      throw Error(ErrorCode::LogMismatch, log,
        "SetRange: hole found, current first index={0} < set index={1}"
        , curfirst, index);
    }
  }

  // GetNodeState returns the persistent state of the node
  pbState GetNodeState()
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return state_;
  }

  // SetNodeState sets the persistent state known to LogReader.
  void SetNodeState(const pbState &state)
  {
    std::lock_guard<std::mutex> gurad(mutex_);
    state_ = state;
  }

  // GetSnapshot returns the metadata for the most recent snapshot known to the
  // LogReader.
  pbSnapshotSPtr GetSnapshot()
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return snapshot_;
  }

  // SetSnapshot makes the snapshot known to LogReader,
  // keeps the metadata of the specified snapshot.
  Status SetSnapshot(pbSnapshotSPtr s)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (snapshot_->index() >= s->index()) {
      return ErrorCode::SnapshotOutOfDate;
    }
    snapshot_ = std::move(s);
    return ErrorCode::OK;
  }

  // ApplySnapshot makes the snapshot known to LogReader,
  // updates the entry range known to LogReader (apply the specified snapshot).
  Status ApplySnapshot(pbSnapshotSPtr s)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (snapshot_->index() >= s->index()) {
      return ErrorCode::SnapshotOutOfDate;
    }
    markerIndex_ = s->index();
    markerTerm_ = s->term();
    length_ = 1;
    snapshot_ = std::move(s);
    return ErrorCode::OK;
  }

  // Term returns the corresponding entry term of a specified entry index
  StatusWith<uint64_t> Term(uint64_t index)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return term(index);
  }

  // GetEntries returns entries between [low, high) with total size of entries
  // limited to maxSize bytes.
  // entries should be an empty vector by design
  Status GetEntries(EntryVector &entries,
    uint64_t low, uint64_t high, uint64_t maxSize)
  {
    assert(entries.empty());
    if (low > high) {
      return ErrorCode::OutOfRange;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    return getEntries(entries, low, high, maxSize);
  }

  // Compact performs entry range compaction on LogReader up to the entry
  // specified by index.
  Status Compact(uint64_t index)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (index < markerIndex_) {
      return ErrorCode::LogCompacted;
    }
    if (index > lastIndex()) {
      return ErrorCode::LogUnavailable;
    }
    auto _term = term(index);
    if (!_term.IsOK()) {
      return _term.Code();
    }
    length_ -= index - markerIndex_;
    markerIndex_ = index;
    markerTerm_ = _term.GetOrThrow();
    return ErrorCode::OK;
  }

  // Append marks the specified entries as persisted and make them available
  // from logreader. This is not how entries are persisted.
  Status Append(Span<pbEntry> entries)
  {
    if (entries.empty()) {
      return ErrorCode::OK;
    }
    if (entries.front().index()+entries.size()-1 != entries.back().index()) {
      throw Error(ErrorCode::LogMismatch, log, "LogReader::Append: gap found");
    }
    SetRange(entries.front().index(), entries.size());
    return ErrorCode::OK;
  }
 private:
  LogReader(uint64_t clusterID, uint64_t nodeID, LogDBSPtr logdb)
    : mutex_(),
      clusterID_(clusterID),
      nodeID_(nodeID),
      cn_(FmtClusterNode(clusterID_, nodeID_)),
      logdb_(std::move(logdb)),
      state_(),
      snapshot_(),
      markerIndex_(0),
      markerTerm_(0),
      length_(1)
  {}

  // assume locked
  std::string describe() const
  {
    return fmt::format(
      "LogReader[markerIndex={0},markerTerm={1},length={2}] {3}",
      markerIndex_, markerTerm_, length_, cn_);
  }

  // assume locked
  uint64_t firstIndex() const
  {
    return markerIndex_ + 1;
  }

  // assume locked
  uint64_t lastIndex() const
  {
    return markerIndex_ + length_ - 1;
  }

  // assume locked
  StatusWith<uint64_t> term(uint64_t index) const
  {
    if (index == markerIndex_) {
      return markerTerm_;
    }
    EntryVector entries;
    Status s = getEntries(entries, index, index + 1, 0); // maxSize = 0 to ensure only 1 entry
    if (!s.IsOK()) {
      return s;
    }
    if (entries.empty()) {
      return 0;
    } else {
      return entries[0]->term();
    }
  }

  // assume locked
  Status getEntries(EntryVector &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const
  {
    if (low <= markerIndex_) {
      return ErrorCode::LogCompacted;
    }
    if (high > lastIndex()+1) {
      return ErrorCode::LogUnavailable;
    }
    uint64_t size = 0;
    StatusWith<uint64_t> _size = logdb_->GetEntries(
      entries, clusterID_, nodeID_, low, high, maxSize);
    if (!_size.IsOK()) {
      return _size.Code();
    }
    size = _size.GetOrDefault(0);
    if (entries.size() == high - low || size > maxSize) { // at least one entry even size > maxSize
      return ErrorCode::OK;
    }
    if (!entries.empty()) {
      if (entries.front()->index() > low) {
        return ErrorCode::LogCompacted;
      }
      uint64_t expected = entries.back()->index() + 1;
      if (lastIndex() <= expected) { // FIXME Is it possible?
        log->error("{0} found log unavailable, "
                   "low={1}, high={2}, lastIndex={3}, expected={4}",
                   describe(), low, high, lastIndex(), expected);
        return ErrorCode::LogUnavailable;
      }
      log->error("{0} found log gap between [{1}:{2}) at {3}",
                 describe(), low, high, expected); // FIXME Is it a gap?
      return ErrorCode::LogMismatch;
    }
    log->warn("{0} failed to get anything from LogDB", describe());
    return ErrorCode::LogUnavailable;
  }

  slogger log;
  std::mutex mutex_;
  uint64_t clusterID_;
  uint64_t nodeID_;
  const std::string cn_;
  LogDBSPtr logdb_;
  pbState state_;
  pbSnapshotSPtr snapshot_;
  uint64_t markerIndex_; // different from that of InMemory::markerIndex
  uint64_t markerTerm_;
  uint64_t length_; // initialized to 1 representing the dummy entry

};
using LogReaderSPtr = std::shared_ptr<LogReader>;

} // namespace logdb

} // namespace ycrt

#endif //YCRT_RAFT_LOGREADER_H_
