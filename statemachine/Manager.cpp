//
// Created by jason on 2020/2/10.
//

#include "Manager.h"

namespace ycrt
{

namespace statemachine
{

using namespace std;

StatusWith<uint64_t> Manager::Open()
{
  return sm_->Open(stopped_);
}

Status Manager::Update(std::vector<Entry> &entries)
{
  return sm_->Update(entries);
}

StatusWith<any> Manager::Lookup(any query)
{
  lock_guard<mutex> guard(mutex_);
  // FIXME: if destroyed
  return sm_->Lookup(std::move(query));
}

Status Manager::Sync()
{
  return sm_->Sync();
}

StatusWith<any> Manager::PrepareSnapshot()
{
  return sm_->PrepareSnapshot();
}

StatusWith<bool> Manager::SaveSnapshot(
  SnapshotMeta &meta,
  SnapshotWriter &writer,
  SnapshotFileSet &files)
{
  StatusWith<uint64_t> s = writer.Write(meta.Session);
  if (config_->IsWitness ||
    (sm_->IsOnDiskStateMachine() && !meta.Request.IsExported())) {
    return {true, s.Code()};
  }
  Status ret = sm_->SaveSnapshot(
    std::move(meta.Context), writer, files, stopped_);
  return {false, ret.Code()};
}

Status Manager::RecoverFromSnapshot(
  SnapshotReader &reader,
  const std::vector<SnapshotFile> &files)
{
  return sm_->RecoverFromSnapshot(reader, files, stopped_);
}

void Manager::Loaded(uint64_t from)
{
  // FIXME
}

void Manager::OffLoaded(uint64_t from)
{
  // FIXME
}

bool Manager::IsRegularStateMachine() const
{
  return sm_->IsRegularStateMachine();
}

bool Manager::IsConcurrentStateMachine() const
{
  return sm_->IsConcurrentStateMachine();
}

bool Manager::IsOnDiskStateMachine() const
{
  return sm_->IsOnDiskStateMachine();
}

pbStateMachineType Manager::StateMachineType() const
{
  return sm_->StateMachineType();
}

} // namespace statemachine

} // namespace ycrt