//
// Created by jason on 2019/12/31.
//

#include "SnapshotChunkManager.h"
#include "settings/Soft.h"

namespace ycrt
{

namespace transport
{

using namespace std;
using namespace settings;

unique_ptr<SnapshotChunkManager> SnapshotChunkManager::New(
  Transport *transport,
  function<string(uint64_t, uint64_t)> &&snapshotDirFunc)
{
  unique_ptr<SnapshotChunkManager> manager(new SnapshotChunkManager());
  manager->getSnapshotDir_ = std::move(snapshotDirFunc);
  return manager;
}

SnapshotChunkManager::SnapshotChunkManager()
  : log(Log.GetLogger("transport")),
    currentTick_(0),
    validate_(true),
    timeoutTick_(Soft::ins().SnapshotChunkTimeoutTick),
    gcTick_(Soft::ins().SnapshotGCTick),
    maxConcurrentSlot_(Soft::ins().MaxConcurrentStreamingSnapshot)
{
}

} // namespace transport

} // namespace ycrt