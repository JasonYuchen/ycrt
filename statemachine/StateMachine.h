//
// Created by jason on 2020/2/10.
//

#ifndef YCRT_STATEMACHINE_STATEMACHINE_H_
#define YCRT_STATEMACHINE_STATEMACHINE_H_

#include <atomic>
#include <boost/filesystem.hpp>

#include "pb/RaftMessage.h"
#include "utils/Utils.h"
#include "ycrt/StateMachine.h"

namespace ycrt
{

namespace statemachine
{

// StateMachine is an adapter interface for underlying StateMachine
// (Regular/Concurrent/OnDisk StateMachine)
class StateMachine {
 public:
  virtual StatusWith<uint64_t> Open(std::atomic_bool &stopped) = 0;
  virtual StatusWith<uint64_t> Update(Span<pbEntrySPtr> entries) = 0;
  virtual StatusWith<any> Lookup(any query) = 0;
  virtual Status Sync() = 0;
  virtual StatusWith<any> PrepareSnapshot() = 0;
  virtual Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) = 0;
  virtual Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) = 0;
  virtual Status Close() = 0;
  virtual bool IsRegularStateMachine() const = 0;
  virtual bool IsConcurrentStateMachine() const = 0;
  virtual bool IsOnDiskStateMachine() const = 0;
  virtual pbStateMachineType StateMachineType() = 0;
};

// TODO: Status StateMachineManager::StreamSnapshot(any context, SnapshotWriter &writer);
//
class Regular : public StateMachine {
 public:
  StatusWith<uint64_t> Open(std::atomic_bool &stopped) override;
  StatusWith<uint64_t> Update(Span<pbEntrySPtr> entries) override;
  StatusWith<any> Lookup(any query) override;
  Status Sync() override;
  StatusWith<any> PrepareSnapshot() override;
  Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) override;
  Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) override;
  Status Close() override;
  bool IsRegularStateMachine() const override;
  bool IsConcurrentStateMachine() const override;
  bool IsOnDiskStateMachine() const override;
  pbStateMachineType StateMachineType() override;
 private:
  std::unique_ptr<ycrt::RegularStateMachine> sm_;
};

class Concurrent : public StateMachine {
 public:
  StatusWith<uint64_t> Open(std::atomic_bool &stopped) override;
  StatusWith<uint64_t> Update(Span<pbEntrySPtr> entries) override;
  StatusWith<any> Lookup(any query) override;
  Status Sync() override;
  StatusWith<any> PrepareSnapshot() override;
  Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) override;
  Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) override;
  Status Close() override;
  bool IsRegularStateMachine() const override;
  bool IsConcurrentStateMachine() const override;
  bool IsOnDiskStateMachine() const override;
  pbStateMachineType StateMachineType() override;
 private:
  std::unique_ptr<ycrt::ConcurrentStateMachine> sm_;
};

class OnDisk : public StateMachine {
 public:
  StatusWith<uint64_t> Open(std::atomic_bool &stopped) override;
  StatusWith<uint64_t> Update(Span<pbEntrySPtr> entries) override;
  StatusWith<any> Lookup(any query) override;
  Status Sync() override;
  StatusWith<any> PrepareSnapshot() override;
  Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) override;
  Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) override;
  Status Close() override;
  bool IsRegularStateMachine() const override;
  bool IsConcurrentStateMachine() const override;
  bool IsOnDiskStateMachine() const override;
  pbStateMachineType StateMachineType() override;
 private:
  std::unique_ptr<ycrt::OnDiskStateMachine> sm_;
};


} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_STATEMACHINE_H_
