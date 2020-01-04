//
// Created by jason on 2020/1/4.
//

#ifndef YCRT_RAFT_REMOTE_H_
#define YCRT_RAFT_REMOTE_H_

#include <stdint.h>

namespace ycrt
{

namespace raft
{

class Remote {
 public:
  enum State : uint8_t {
    Retry = 0, Wait, Replicate, Snapshot, NumOfState
  };
  void Reset();
  void BecomeRetry();
  void BecomeWait();
  void BecomeReplicate();
  void BecomeSnapshot(uint64_t index);
  void ClearPendingSnapshot();
  bool TryUpdate(uint64_t index);
  void Progress(uint64_t lastIndex);
  void RespondedTo();
  bool DecreaseTo(uint64_t rejected, uint64_t last);
  bool IsPaused();
  bool IsActive();
  void SetActive(bool isActive);
  void RetryToWait();
  void WaitToRetry();
 private:
  bool active_;
  State state_;
  uint64_t match_;
  uint64_t next_;
  uint64_t snapshotIndex_;
};

const char *StateToString(Remote::State state);

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_REMOTE_H_
