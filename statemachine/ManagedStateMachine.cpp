//
// Created by jason on 2020/2/10.
//

#include "ManagedStateMachine.h"

namespace ycrt
{

namespace statemachine
{

StatusWith<uint64_t> Regular::Open(std::atomic_bool &stopped)
{
  throw Error(ErrorCode::Other, "Regular::Open: not supported");
}

Status Regular::Update(std::vector<Entry> &entries)
{
  if (entries.size() != 1) {
    throw Error(ErrorCode::Other, "Regular::Update: size != 1");
  }
  return sm_->Update(entries[0]);
}

StatusWith<any> Regular::Lookup(any query)
{
  return sm_->Lookup(std::move(query));
}

Status Regular::Sync()
{
  throw Error(ErrorCode::Other, "Regular::Sync: not supported");
}

StatusWith<any> Regular::PrepareSnapshot()
{
  throw Error(ErrorCode::Other, "Regular::PrepareSnapshot: not supported");
}

Status Regular::SaveSnapshot(
  any context,
  SnapshotWriter &writer,
  SnapshotFileSet &files,
  std::atomic_bool &stopped)
{
  if (!context.empty()) {
    throw Error(ErrorCode::Other, "Regular::SaveSnapshot: context not empty");
  }
  return sm_->SaveSnapshot(writer, files, stopped);
}

Status Regular::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files,
  std::atomic_bool &stopped)
{
  return sm_->RecoverFromSnapshot(reader, files, stopped);
}

Status Regular::Close()
{
  return sm_->Close();
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

pbStateMachineType Regular::StateMachineType() const
{
  return raftpb::RegularStateMachine;
}

StatusWith<uint64_t> Concurrent::Open(std::atomic_bool &stopped)
{
  throw Error(ErrorCode::Other, "Concurrent::Open: not supported");
}

Status Concurrent::Update(std::vector<Entry> &entries)
{
  return sm_->Update(entries);
}

StatusWith<any> Concurrent::Lookup(any query)
{
  return sm_->Lookup(std::move(query));
}

Status Concurrent::Sync()
{
  throw Error(ErrorCode::Other, "Concurrent::Sync: not supported");
}

StatusWith<any> Concurrent::PrepareSnapshot()
{
  return sm_->PrepareSnapshot();
}

Status Concurrent::SaveSnapshot(
  any context,
  SnapshotWriter &writer,
  SnapshotFileSet &files,
  std::atomic_bool &stopped)
{
  return sm_->SaveSnapshot(std::move(context), writer, files, stopped);
}

Status Concurrent::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files,
  std::atomic_bool &stopped)
{
  return sm_->RecoverFromSnapshot(reader, files, stopped);
}

Status Concurrent::Close()
{
  return sm_->Close();
}

bool Concurrent::IsRegularStateMachine() const
{
  return false;
}

bool Concurrent::IsConcurrentStateMachine() const
{
  return true;
}

bool Concurrent::IsOnDiskStateMachine() const
{
  return false;
}

pbStateMachineType Concurrent::StateMachineType() const
{
  return raftpb::ConcurrentStateMachine;
}

StatusWith<uint64_t> OnDisk::Open(std::atomic_bool &stopped)
{
  if (opened_) {
    throw Error(ErrorCode::Other, "OnDisk::Open: already opened");
  }
  StatusWith<uint64_t> result = sm_->Open(stopped);
  if (!result.IsOK()) {
    return {0, result.Code()};
  }
  return result;
}

Status OnDisk::Update(std::vector<Entry> &entries)
{
  if (!opened_) {
    throw Error(ErrorCode::Other, "OnDisk::Update: not opened");
  }
  return sm_->Update(entries);
}

StatusWith<any> OnDisk::Lookup(any query)
{
  if (!opened_) {
    throw Error(ErrorCode::Other, "OnDisk::Lookup: not opened");
  }
  return sm_->Lookup(std::move(query));
}

Status OnDisk::Sync()
{
  if (!opened_) {
    throw Error(ErrorCode::Other, "OnDisk::Sync: not opened");
  }
  return sm_->Sync();
}

StatusWith<any> OnDisk::PrepareSnapshot()
{
  if (!opened_) {
    throw Error(ErrorCode::Other, "OnDisk::PrepareSnapshot: not opened");
  }
  return sm_->PrepareSnapshot();
}

Status OnDisk::SaveSnapshot(
  any context,
  SnapshotWriter &writer,
  SnapshotFileSet &files,
  std::atomic_bool &stopped)
{
  if (!opened_) {
    throw Error(ErrorCode::Other, "OnDisk::SaveSnapshot: not opened");
  }
  return sm_->SaveSnapshot(std::move(context), writer, files, stopped);
}

Status OnDisk::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files,
  std::atomic_bool &stopped)
{
  if (!opened_) {
    throw Error(ErrorCode::Other, "OnDisk::RecoverFromSnapshot: not opened");
  }
  return sm_->RecoverFromSnapshot(reader, files, stopped);
}

Status OnDisk::Close()
{
  return sm_->Close();
}

bool OnDisk::IsRegularStateMachine() const
{
  return false;
}

bool OnDisk::IsConcurrentStateMachine() const
{
  return true;
}

bool OnDisk::IsOnDiskStateMachine() const
{
  return true;
}

pbStateMachineType OnDisk::StateMachineType() const
{
  return raftpb::OnDiskStateMachine;
}

} // namespace statemachine

} // namespace ycrt