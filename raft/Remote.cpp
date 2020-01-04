//
// Created by jason on 2020/1/4.
//

#include "Remote.h"
#include <assert.h>
#include <algorithm>
#include "utils/Error.h"

namespace ycrt
{

namespace raft
{

using namespace std;

void Remote::Reset()
{
  snapshotIndex_ = 0;
}

void Remote::BecomeRetry()
{
  if (state_ == Snapshot) {
    next_ = max(match_ + 1, snapshotIndex_ + 1);
  } else {
    next_ = match_ + 1;
  }
  Reset();
  state_ = Retry;
}

void Remote::BecomeWait()
{
  BecomeRetry();
  RetryToWait();
}

void Remote::BecomeReplicate()
{
  next_ = match_ + 1;
  Reset();
  state_ = Replicate;
}

void Remote::BecomeSnapshot(uint64_t index)
{
  Reset();
  snapshotIndex_ = index;
  state_ = Snapshot;
}

void Remote::ClearPendingSnapshot()
{
  snapshotIndex_ = 0;
}

bool Remote::TryUpdate(uint64_t index)
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

void Remote::Progress(uint64_t lastIndex)
{
  if (state_ == Replicate) {
    next_ = lastIndex + 1;
  } else if (state_ == Retry) {
    RetryToWait();
  } else {
    throw Fatal(errRemoteState, "unexpected remote state");
  }
}

void Remote::RespondedTo()
{
  if (state_ == Retry) {
    BecomeReplicate();
  } else if (state_ == Snapshot) {
    if (match_ >= snapshotIndex_) {
      BecomeRetry();
    }
  }
}

bool Remote::DecreaseTo(uint64_t rejected, uint64_t last)
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
  next_ = max(1UL, min(rejected, last + 1));
  return true;
}

bool Remote::IsPaused()
{
  return (state_ == Wait || state_ == Snapshot);
}

bool Remote::IsActive()
{
  return active_;
}

void Remote::SetActive(bool isActive)
{
  active_ = isActive;
}

void Remote::RetryToWait()
{
  if (state_ == Retry) {
    state_ = Wait;
  }
}

void Remote::WaitToRetry()
{
  if (state_ == Wait) {
    state_ = Retry;
  }
}

const char *StateToString(Remote::State state)
{
  static const char *states[] =
    {"Retry", "Wait", "Replicate", "Snapshot"};
  assert(state < Remote::NumOfState);
  return states[state];
}

} // namespace raft

} // namespace ycrt