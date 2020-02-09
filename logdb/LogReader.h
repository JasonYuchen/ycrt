//
// Created by jason on 2020/2/1.
//

#ifndef YCRT_RAFT_LOGREADER_H_
#define YCRT_RAFT_LOGREADER_H_

#include <stdint.h>
#include <vector>
#include <memory>
#include <mutex>
#include <settings/Soft.h>
#include "utils/Utils.h"
#include "pb/RaftMessage.h"
#include "LogDB.h"

namespace ycrt
{

namespace logdb
{

// LogReader is a read-only interface to the underlying persistent storage to
// allow the raft package to access raft state, entries, snapshots stored in
// the persistent storage. Entries stored in the persistent storage accessible
// via LogReader is usually not required in normal cases.
class LogReader {
 public:
  static std::unique_ptr<LogReader> New(NodeInfo node, LogDBSPtr logdb);
  DISALLOW_COPY_AND_ASSIGN(LogReader);

  // GetRange returns the range of the entries in LogReader.
  std::pair<uint64_t, uint64_t> GetRange();

  // SetRange updates the LogReader to reflect what is available in it.
  void SetRange(uint64_t index, uint64_t length);

  // GetNodeState returns the persistent state of the node
  pbState GetNodeState();

  // SetNodeState sets the persistent state known to LogReader.
  void SetNodeState(const pbState &state);

  // GetSnapshot returns the metadata for the most recent snapshot known to the
  // LogReader.
  pbSnapshotSPtr GetSnapshot();

  // SetSnapshot makes the snapshot known to LogReader,
  // keeps the metadata of the specified snapshot.
  Status SetSnapshot(pbSnapshotSPtr s);

  // ApplySnapshot makes the snapshot known to LogReader,
  // updates the entry range known to LogReader (apply the specified snapshot).
  Status ApplySnapshot(pbSnapshotSPtr s);

  // Term returns the corresponding entry term of a specified entry index
  StatusWith<uint64_t> Term(uint64_t index);

  // GetEntries returns entries between [low, high) with total size of entries
  // limited to maxSize bytes.
  // entries should be an empty vector by design
  Status GetEntries(
    EntryVector &entries,
    uint64_t low,
    uint64_t high,
    uint64_t maxSize);

  // Compact performs entry range compaction on LogReader up to the entry
  // specified by index.
  Status Compact(uint64_t index);

  // Append marks the specified entries as persisted and make them available
  // from logreader. This is not how entries are persisted.
  Status Append(Span<pbEntry> entries);
 private:
  LogReader(NodeInfo node, LogDBSPtr logdb);

  // assume locked
  std::string describe() const;

  // assume locked
  uint64_t firstIndex() const;

  // assume locked
  uint64_t lastIndex() const;

  // assume locked
  StatusWith<uint64_t> term(uint64_t index) const;

  // assume locked
  Status getEntries(EntryVector &entries,
    uint64_t low, uint64_t high, uint64_t maxSize) const;

  slogger log;
  std::mutex mutex_;
  NodeInfo node_;
  const std::string nodeDesc_;
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
