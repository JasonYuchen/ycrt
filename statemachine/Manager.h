//
// Created by jason on 2020/2/10.
//

#ifndef YCRT_STATEMACHINE_MANAGER_H_
#define YCRT_STATEMACHINE_MANAGER_H_

#include "SnapshotIO.h"
#include "ManagedStateMachine.h"
#include "ycrt/Config.h"

namespace ycrt
{

namespace statemachine
{

// Manager is used to manage ManagedStateMachine
class Manager {
 public:
  Manager(std::atomic_bool &stopped) : stopped_(stopped) { /* TODO */ }

  // Open opens on disk state machine.
  StatusWith<uint64_t> Open();

  // Update updates the data store.
  Status Update(std::vector<Entry> &entries);

  // Lookup queries the data store.
  StatusWith<any> Lookup(any query);

  // Sync synchronizes state machine's in-core state with that on disk.
  Status Sync();

  // PrepareSnapshot makes preparation for concurrently taking snapshot.
  StatusWith<any> PrepareSnapshot();

  // SaveSnapshot saves the state of the data store to the snapshot file specified
  // by the fp input string.
  //  meta.Context (returned by PrepareSnapshot) will be owned after this call
  StatusWith<bool> SaveSnapshot(
    SnapshotMeta &meta,
    SnapshotWriter &writer,
    SnapshotFileSet &files);

  // RecoverFromSnapshot recovers the state of the data store from the snapshot
  // file specified by the fp input string.
  Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files);

  // StreamSnapshot creates and streams snapshot to a remote node.
  // TODO: Status StreamSnapshot(any context, SnapshotWriter &writer);

  // Loaded marks the statemachine as loaded by the specified component.
  void Loaded(uint64_t from);

  // Offloaded offloads the data store from the specified part of the system.
  void OffLoaded(uint64_t from);
  bool IsRegularStateMachine() const;
  bool IsConcurrentStateMachine() const;
  bool IsOnDiskStateMachine() const;
  pbStateMachineType StateMachineType() const;
 private:
  std::mutex mutex_;
  std::atomic_bool &stopped_;
  ConfigSPtr config_;
  // TODO: OffloadedStatus ?
  std::unique_ptr<ManagedStateMachine> sm_;
};

} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_MANAGER_H_
