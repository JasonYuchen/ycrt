//
// Created by jason on 2020/1/4.
//

#ifndef YCRT_RAFT_REMOTE_H_
#define YCRT_RAFT_REMOTE_H_

#include <assert.h>
#include <stdint.h>
#include <spdlog/fmt/ostr.h>

#include "utils/Error.h"

namespace ycrt
{

namespace raft
{

class Remote {
 public:
  Remote() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(Remote);

  enum State : uint8_t {
    Retry = 0, Wait, Replicate, Snapshot, NumOfState
  };
  bool Active() { return active_; }
  Remote &SetActive(bool active) { active_ = active; return *this; }
  State State() { return state_; }
  Remote &SetState(enum State state) { state_ = state; return *this; }
  uint64_t Match() { return match_; }
  Remote &SetMatch(uint64_t match) { match_ = match; return *this; }
  uint64_t Next() { return next_; }
  Remote &SetNext(uint64_t next) { next_ = next; return *this; }
  uint64_t SnapshotIndex() { return snapshotIndex_; }
  Remote &SetSnapshotIndex(uint64_t idx) { snapshotIndex_ = idx; return *this; }

  void Reset() { snapshotIndex_ = 0; }

  void BecomeRetry()
  {
    if (state_ == Snapshot) {
      next_ = std::max(match_ + 1, snapshotIndex_ + 1);
    } else {
      next_ = match_ + 1;
    }
    Reset();
    state_ = Retry;
  }

  void BecomeWait()
  {
    BecomeRetry();
    RetryToWait();
  }

  void BecomeReplicate()
  {
    next_ = match_ + 1;
    Reset();
    state_ = Replicate;
  }

  void BecomeSnapshot(uint64_t index)
  {
    Reset();
    snapshotIndex_ = index;
    state_ = Snapshot;
  }

  void ClearPendingSnapshot() { snapshotIndex_ = 0; }

  bool TryUpdate(uint64_t index)
  {
    if (next_ < index + 1) {
      next_ = index + 1;
    }
    if (match_ < index) {
      WaitToRetry();
      match_ = index;
      return true;
    }
    return false;
  }

  void Progress(uint64_t lastIndex)
  {
    if (state_ == Replicate) {
      next_ = lastIndex + 1;
    } else if (state_ == Retry) {
      RetryToWait();
    } else {
      throw Error(ErrorCode::RemoteState, "unexpected remote state");
    }
  }

  void RespondedTo()
  {
    if (state_ == Retry) {
      BecomeReplicate();
    } else if (state_ == Snapshot) {
      if (match_ >= snapshotIndex_) {
        BecomeRetry();
      }
    }
  }

  bool DecreaseTo(uint64_t rejected, uint64_t last)
  {
    if (state_ == Replicate) {
      if (rejected <= match_) {
        return false;
      }
      next_ = match_ + 1;
      return true;
    }
    if (next_ - 1 != rejected) {
      return false;
    }
    WaitToRetry();
    next_ = std::max(1UL, std::min(rejected, last + 1));
    return true;
  }

  bool IsPaused() { return state_ == Wait || state_ == Snapshot; }

  void RetryToWait()
  {
    if (state_ == Retry) {
      state_ = Wait;
    }
  }

  void WaitToRetry()
  {
    if (state_ == Wait) {
      state_ = Retry;
    }
  }

  void EnterRetry()
  {
    if (state_ == Replicate) {
      BecomeRetry();
    }
  }

 private:
  template<typename os>
  friend os &operator<<(os &o, const Remote &r);

  bool active_ = false;
  enum State state_ = Retry;
  uint64_t match_ = 0;
  uint64_t next_ = 0;
  uint64_t snapshotIndex_ = 0;
};

inline const char *StateToString(enum Remote::State state)
{
  static const char *states[] =
    {"Retry", "Wait", "Replicate", "Snapshot"};
  assert(state < Remote::NumOfState);
  return states[state];
}

// for spdlog
template<typename os>
os &operator<<(os &o, const Remote &r)
{
  return o << "Remote"
    "[Active=" << r.active_ <<
    ",State=" << StateToString(r.state_) <<
    ",Match=" << r.match_ <<
    ",Next=" << r.next_ <<
    ",SnapshotIndex=" << r.snapshotIndex_ << "]";
}

} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_REMOTE_H_
