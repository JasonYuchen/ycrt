//
// Created by jason on 2019/12/31.
//

#include "SnapshotChunk.h"
#include "settings/Soft.h"

namespace ycrt
{

namespace transport
{

using namespace std;
using namespace settings;

unique_ptr<SnapshotChunk> SnapshotChunk::New(
  function<void(MessageBatchUPtr)> onReceive,
  function<void(uint64_t, uint64_t, uint64_t)> confirm,
  function<uint64_t()> deploymentIDFunc,
  function<string(uint64_t, uint64_t)> snapshotDirFunc)
{
  auto chunk = make_unique<SnapshotChunk>();
  chunk->onReceive_ = std::move(onReceive);
  chunk->confirm_ = std::move(confirm);
  chunk->getDeploymentID_ = std::move(deploymentIDFunc);
  chunk->getSnapshotDir_ = std::move(snapshotDirFunc);
  return chunk;
}

SnapshotChunk::SnapshotChunk()
  : currentTick_(0),
    validate_(true),
    timeoutTick_(Soft::ins().SnapshotChunkTimeoutTick),
    gcTick_(Soft::ins().SnapshotGCTick),
    maxConcurrentSlot_(Soft::ins().MaxConcurrentStreamingSnapshot)
{
}

} // namespace transport

} // namespace ycrt