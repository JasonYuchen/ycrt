//
// Created by jason on 2020/2/10.
//

#include "StateMachine.h"

namespace ycrt
{

namespace statemachine
{

StatusWith<uint64_t> Regular::Open(std::atomic_bool &stopped)
{
  return StatusWith<uint64_t>(<#initializer#>);
}
StatusWith<uint64_t> Regular::Update(Span<pbEntrySPtr> entries)
{
  return StatusWith<uint64_t>(<#initializer#>);
}
StatusWith<any> Regular::Lookup(any query)
{
  return StatusWith<any>(fundamentals_v1::any());
}
Status Regular::Sync()
{
  return Status();
}
StatusWith<any> Regular::PrepareSnapshot()
{
  return StatusWith<any>(fundamentals_v1::any());
}
Status Regular::SaveSnapshot(
  any context,
  SnapshotWriter &writer,
  SnapshotFileSet &files,
  std::atomic_bool &stopped)
{
  return Status();
}
Status Regular::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files,
  std::atomic_bool &stopped)
{
  return Status();
}
Status Regular::Close()
{
  return Status();
}

bool Regular::IsRegularStateMachine() const
{
  return true;
}

bool Regular::IsConcurrentStateMachine() const
{
  return false;
}

bool Regular::IsOnDiskStateMachine() const
{
  return false;
}

pbStateMachineType Regular::StateMachineType()
{
  return raftpb::RegularStateMachine;
}
StatusWith<uint64_t> Concurrent::Open(std::atomic_bool &stopped)
{
  return StatusWith<uint64_t>(<#initializer#>);
}
StatusWith<uint64_t> Concurrent::Update(Span<pbEntrySPtr> entries)
{
  return StatusWith<uint64_t>(<#initializer#>);
}
StatusWith<any> Concurrent::Lookup(any query)
{
  return StatusWith<any>(fundamentals_v1::any());
}
Status Concurrent::Sync()
{
  return Status();
}
StatusWith<any> Concurrent::PrepareSnapshot()
{
  return StatusWith<any>(fundamentals_v1::any());
}
Status Concurrent::SaveSnapshot(
  any context,
  SnapshotWriter &writer,
  SnapshotFileSet &files,
  std::atomic_bool &stopped)
{
  return Status();
}
Status Concurrent::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files,
  std::atomic_bool &stopped)
{
  return Status();
}
Status Concurrent::Close()
{
  return Status();
}
bool Concurrent::IsRegularStateMachine() const
{
  return false;
}
bool Concurrent::IsConcurrentStateMachine() const
{
  return false;
}
bool Concurrent::IsOnDiskStateMachine() const
{
  return false;
}
pbStateMachineType Concurrent::StateMachineType()
{
  return StateMachineType_INT_MAX_SENTINEL_DO_NOT_USE_;
}
StatusWith<uint64_t> OnDisk::Open(std::atomic_bool &stopped)
{
  return StatusWith<uint64_t>(<#initializer#>);
}
StatusWith<uint64_t> OnDisk::Update(Span<pbEntrySPtr> entries)
{
  return StatusWith<uint64_t>(<#initializer#>);
}
StatusWith<any> OnDisk::Lookup(any query)
{
  return StatusWith<any>(fundamentals_v1::any());
}
Status OnDisk::Sync()
{
  return Status();
}
StatusWith<any> OnDisk::PrepareSnapshot()
{
  return StatusWith<any>(fundamentals_v1::any());
}
Status OnDisk::SaveSnapshot(
  any context,
  SnapshotWriter &writer,
  SnapshotFileSet &files,
  std::atomic_bool &stopped)
{
  return Status();
}
Status OnDisk::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files,
  std::atomic_bool &stopped)
{
  return Status();
}
Status OnDisk::Close()
{
  return Status();
}
bool OnDisk::IsRegularStateMachine() const
{
  return false;
}
bool OnDisk::IsConcurrentStateMachine() const
{
  return false;
}
bool OnDisk::IsOnDiskStateMachine() const
{
  return false;
}
pbStateMachineType OnDisk::StateMachineType()
{
  return StateMachineType_INT_MAX_SENTINEL_DO_NOT_USE_;
}
} // namespace statemachine

} // namespace ycrt