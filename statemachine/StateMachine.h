//
// Created by jason on 2020/2/10.
//

#ifndef YCRT_STATEMACHINE_STATEMACHINE_H_
#define YCRT_STATEMACHINE_STATEMACHINE_H_

#include <atomic>
#include <boost/filesystem.hpp>

#include "pb/RaftMessage.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace statemachine
{

class SnapshotWriter {
 public:
  StatusWith<uint64_t> Write(string_view content);
 private:
};

class SnapshotReader {
 public:
  StatusWith<uint64_t> Read(std::string &content);
 private:
};

class SnapshotFileSet {
 public:
  void AddFile(
    uint64_t fileID,
    boost::filesystem::path path,
    string_view metadata);
};

// TODO: Status StateMachineManager::StreamSnapshot(any context, SnapshotWriter &writer);

// StateMachine is an adapter interface for underlying StateMachine
// (Regular/Concurrent/OnDisk StateMachine)
class StateMachine {
 public:
  StatusWith<uint64_t> Open(std::atomic_bool &stopped);
  StatusWith<uint64_t> Update(EntryVector &entries);
  StatusWith<any> Lookup(any query);
  Status Sync();
  StatusWith<any> PrepareSnapshot();
  Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped);
  Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const SnapshotFileSet &files);
  Status Close();
  bool IsRegularStateMachine();
  bool IsConcurrentStateMachine();
  bool IsOnDiskStateMachine();
  pbStateMachineType StateMachineType();
 private:
  // TODO: ycrt::StateMachineUPtr
};



} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_STATEMACHINE_H_
