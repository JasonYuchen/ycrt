//
// Created by jason on 2019/12/31.
//

#include "SnapshotChunkManager.h"
#include "settings/Soft.h"
#include "Transport.h"

namespace ycrt
{

namespace transport
{

using namespace std;
using namespace settings;

// TODO: move to class SnapshotChunk
static string GetSnapshotKey(const pbSnapshotChunk &chunk)
{
  return fmt::format("{0}:{1}:{2}",
    chunk.cluster_id(), chunk.node_id(), chunk.index());
}

unique_ptr<SnapshotChunkManager> SnapshotChunkManager::New(
  Transport &transport,
  function<string(uint64_t, uint64_t)> &&getSnapshotDir)
{
  unique_ptr<SnapshotChunkManager> manager(
    new SnapshotChunkManager(transport, std::move(getSnapshotDir)));
  return manager;
}

bool SnapshotChunkManager::AddChunk(pbSnapshotChunkSPtr chunk)
{
  // FIXME: check BinVer
  if (chunk->deployment_id() != transport_.GetDeploymentID()) {
    log->error("SnapshotChunkManager::AddChunk: invalid deploymentID, "
               "expected={0}, actual={1}",
               transport_.GetDeploymentID(), chunk->deployment_id());
    return false;
  }
  string snapshotKey = GetSnapshotKey(*chunk);
  shared_ptr<mutex> snapshotLock = getSnapshotLock(snapshotKey);
  lock_guard<mutex> snapshotGuard(*snapshotLock);
  if(!onNewChunk(snapshotKey, chunk)) {
    return false;
  }
  shared_ptr<track> t = tracked_[snapshotKey];
  if (shouldUpdateValidator(*chunk)) {
    // FIXME: validator
  }

}

void SnapshotChunkManager::Tick()
{
  uint64_t tick = currentTick_.fetch_add(1);
  if (tick % gcTick_ == 0) {
    gc();
  }
}

SnapshotChunkManager::SnapshotChunkManager(
  Transport &transport_,
  function<string(uint64_t, uint64_t)> &&getSnapshotDir)
  : timeoutTick_(Soft::ins().SnapshotChunkTimeoutTick),
    gcTick_(Soft::ins().SnapshotGCTick),
    maxConcurrentSlot_(Soft::ins().MaxConcurrentStreamingSnapshot),
    log(Log.GetLogger("transport")),
    transport_(transport_),
    currentTick_(0),
    validate_(true),
    getSnapshotDir_(std::move(getSnapshotDir)),
    mutex_(),
    tracked_(),
    locks_()
{
}

shared_ptr<mutex> SnapshotChunkManager::getSnapshotLock(
  const string &snapshotKey)
{
  lock_guard<mutex> guard(mutex_);
  if (locks_.find(snapshotKey) == locks_.end()) {
    locks_.emplace(snapshotKey, make_shared<mutex>());
  }
  return locks_[snapshotKey];
}

bool SnapshotChunkManager::onNewChunk(
  const string &key,
  pbSnapshotChunkSPtr chunk)
{
  lock_guard<mutex> guard(mutex_);
  auto t = tracked_.find(key);
  if (chunk->chunk_id() == 0) {
    log->info("received the first chunk of a snapshot, key={0}", key);
    if (t != tracked_.end()) {
      log->warn("removing unclaimed chunks, key={0}", key);
      deleteTempChunkDir(*chunk);
      tracked_.erase(t);
    }
    if (tracked_.size() >= maxConcurrentSlot_) {
      log->error("max slot count reached, dropped a chunk, key={0}", key);
      return false;
    }
    // FIXME: auto validator = rsm.NewSnapshotValidator()
    //  if (validate && !chunk->HasFileInfo) {
    //    .....
    //  }
    auto newTrack = make_shared<track>();
    newTrack->firstChunk = chunk;
    // newTrack->validator = validator;
    newTrack->nextChunk = 1;
    newTrack->tick = 0;
    tracked_.emplace(key, std::move(newTrack));
    t = tracked_.find(key);
  } else {
    if (t == tracked_.end()) {
      log->warn("ignored a not tracked chunk, key={0}, id={1}",
        key, chunk->chunk_id());
      return false;
    }
    if (t->second->nextChunk != chunk->chunk_id()) {
      log->warn("ignored an out of order chunk, key={0}, id={1}, expected={2}",
        key, chunk->chunk_id(), t->second->nextChunk);
      return false;
    }
    if (t->second->firstChunk->from() != chunk->from()) {
      log->warn("ignored a chunk, key={0}, from={1}, expected={2}",
        key, chunk->from(), t->second->firstChunk->from());
      return false;
    }
    t->second->nextChunk = chunk->chunk_id() + 1;
  }
  if (chunk->file_chunk_id() == 0 && chunk->has_file_info()) {
    t->second->extraFiles.emplace_back(
      make_shared<pbSnapshotFile>(chunk->file_info()));
  }
  t->second->tick = currentTick_;
  return true;
}

void SnapshotChunkManager::gc()
{
  lock_guard<mutex> guard(mutex_);
  uint64_t tick = currentTick_;
  for (auto it = tracked_.begin(); it != tracked_.end();) {
    if (tick - it->second->tick >= timeoutTick_) {
      deleteTempChunkDir(*it->second->firstChunk);
      it = tracked_.erase(it);
    } else {
      it++;
    }
  }
}

void SnapshotChunkManager::deleteTempChunkDir(const pbSnapshotChunk &chunk)
{
  // TODO
}

bool SnapshotChunkManager::shouldUpdateValidator(const pbSnapshotChunk &chunk)
{
  // TODO
}

} // namespace transport

} // namespace ycrt