//
// Created by jason on 2020/1/3.
//

#include "LogEntry.h"
std::pair<uint64_t, uint64_t> ycrt::raft::LogDB::Range()
{
  return std::pair<uint64_t, uint64_t>();
}
void ycrt::raft::LogDB::SetRange(uint64_t index, uint64_t length)
{

}
std::pair<ycrt::pbStateSPtr,
          ycrt::pbMembershipSPtr> ycrt::raft::LogDB::NodeState()
{
  return std::pair<ycrt::pbStateSPtr, ycrt::pbMembershipSPtr>();
}
void ycrt::raft::LogDB::SetNodeState(ycrt::pbStateSPtr)
{

}
ycrt::Status ycrt::raft::LogDB::CreateSnapshot(ycrt::pbSnapshotUPtr)
{
  return ycrt::Status();
}
ycrt::Status ycrt::raft::LogDB::ApplySnapshot(ycrt::pbSnapshotUPtr)
{
  return ycrt::Status();
}
ycrt::StatusWith<uint64_t> ycrt::raft::LogDB::Term(uint64_t index)
{
  return ErrorCode::OutOfRange;
}
ycrt::StatusWith<std::vector<ycrt::pbEntry>> ycrt::raft::LogDB::Entries(
  uint64_t low,
  uint64_t high,
  uint64_t maxSize)
{
  return ErrorCode::OutOfRange;
}
ycrt::pbSnapshotUPtr ycrt::raft::LogDB::Snapshot()
{
  return ycrt::pbSnapshotUPtr();
}
ycrt::Status ycrt::raft::LogDB::Compact(uint64_t index)
{
  return ycrt::Status();
}
ycrt::Status ycrt::raft::LogDB::Append(ycrt::Span<ycrt::pbEntry> entries)
{
  return ycrt::Status();
}
