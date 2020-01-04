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
  SnapshotIndex = 0;
}

void Remote::BecomeRetry()
{
  if (State == Snapshot) {
    Next = max(Match + 1, SnapshotIndex + 1);
  } else {
    Next = Match + 1;
  }
  Reset();
  State = Retry;
}

void Remote::BecomeWait()
{
  BecomeRetry();
  RetryToWait();
}

void Remote::BecomeReplicate()
{
  Next = Match + 1;
  Reset();
  State = Replicate;
}

void Remote::BecomeSnapshot(uint64_t index)
{
  Reset();
  SnapshotIndex = index;
  State = Snapshot;
}

void Remote::ClearPendingSnapshot()
{
  SnapshotIndex = 0;
}

bool Remote::TryUpdate(uint64_t index)
{
  if (Next < index + 1) {
    Next = index + 1;
  }
  if (Match < index) {
    WaitToRetry();
    Match = index;
    return true;
  }
  return false;
}

void Remote::Progress(uint64_t lastIndex)
{
  if (State == Replicate) {
    Next = lastIndex + 1;
  } else if (State == Retry) {
    RetryToWait();
  } else {
    throw Fatal(errRemoteState, "unexpected remote state");
  }
}

void Remote::RespondedTo()
{
  if (State == Retry) {
    BecomeReplicate();
  } else if (State == Snapshot) {
    if (Match >= SnapshotIndex) {
      BecomeRetry();
    }
  }
}

bool Remote::DecreaseTo(uint64_t rejected, uint64_t last)
{
  if (State == Replicate) {
    if (rejected <= Match) {
      return false;
    }
    Next = Match + 1;
    return true;
  }
  if (Next - 1 != rejected) {
    return false;
  }
  WaitToRetry();
  Next = max(1UL, min(rejected, last + 1));
  return true;
}

bool Remote::IsPaused()
{
  return (State == Wait || State == Snapshot);
}

bool Remote::IsActive()
{
  return Active;
}

void Remote::RetryToWait()
{
  if (State == Retry) {
    State = Wait;
  }
}

void Remote::WaitToRetry()
{
  if (State == Wait) {
    State = Retry;
  }
}


const char *StateToString(enum Remote::State state)
{
  static const char *states[] =
    {"Retry", "Wait", "Replicate", "Snapshot"};
  assert(state < Remote::NumOfState);
  return states[state];
}

} // namespace raft

} // namespace ycrt