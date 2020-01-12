//
// Created by jason on 2020/1/4.
//

#ifndef YCRT_RAFT_REMOTE_H_
#define YCRT_RAFT_REMOTE_H_

#include <stdint.h>
#include <spdlog/fmt/ostr.h>

namespace ycrt
{

namespace raft
{

struct Remote {
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
  void SetActive(bool active);
  void RetryToWait();
  void WaitToRetry();
  void EnterRetry();

  bool Active = false;
  enum State State = Retry;
  uint64_t Match = 0;
  uint64_t Next = 0;
  uint64_t SnapshotIndex = 0;
};

const char *StateToString(enum Remote::State state);

// for spdlog
template<typename os>
os &operator<<(os &o, const ycrt::raft::Remote &r)
{
  return o << "Remote"
    "[Active=" << r.Active <<
    ",State=" << StateToString(r.State) <<
    ",Match=" << r.Match <<
    ",Next=" << r.Next <<
    ",SnapshotIndex=" << r.SnapshotIndex << "]";
}

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_REMOTE_H_
