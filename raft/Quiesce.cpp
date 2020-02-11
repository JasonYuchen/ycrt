//
// Created by jason on 2020/2/11.
//

#include "Quiesce.h"

namespace ycrt
{

namespace raft
{

QuiesceManager::QuiesceManager(
  NodeInfo node,
  bool enable,
  uint64_t electionTick)
  : log(Log.GetLogger("raft")),
    node_(node),
    tick_(0),
    electionTick_(electionTick),
    quiescedThreshold_(electionTick * 10),
    quiescedSince_(0),
    noActivitySince_(0),
    exitQuiesceTick_(0),
    newQuiesceStateFlag_(false),
    enabled_(enable)
{
}

bool QuiesceManager::NewQuiesceState()
{
  return newQuiesceStateFlag_.exchange(false);
}

uint64_t QuiesceManager::IncreaseQuiesceTick()
{
  if (!enabled_) {
    return 0;
  }
  tick_++;
  if (!isQuiesced()) {
    if (tick_ - noActivitySince_ > quiescedThreshold_) {
      log->info("{} enter quiesce", node_);
      quiescedSince_ = tick_;
      noActivitySince_ = tick_;
      newQuiesceStateFlag_ = true;
    }
  }
  return tick_;
}

void QuiesceManager::RecordActivity(pbMessageType type)
{
  if (!enabled_) {
    return;
  }
  if (type == raftpb::Heartbeat || type == raftpb::HeartbeatResp) {
    if (!isQuiesced()) {
      return;
    }
    if (isQuiesced() && tick_ - quiescedSince_ < electionTick_) {
      return;
    }
  }
  noActivitySince_ = tick_;
  if (isQuiesced()) {
    log->info("{} exited from quiesce, message type {}, current tick {}",
      node_, type, tick_);
    quiescedSince_ = 0;
    exitQuiesceTick_ = tick_;
  }
}

void QuiesceManager::TryEnterQuiesce()
{
  if (!isQuiesced() && tick_ - exitQuiesceTick_ < quiescedThreshold_) {
    return;
  }
  if (!isQuiesced()) {
    log->info("{} enter quiesce", node_);
    quiescedSince_ = tick_;
    noActivitySince_ = tick_;
    newQuiesceStateFlag_ = true;
  }
}

bool QuiesceManager::isQuiesced() const
{
  if (!enabled_) {
    return false;
  }
  return quiescedSince_ > 0;
}

} // namespace raft

} // namespace ycrt