//
// Created by jason on 2019/12/21.
//

#include "pb/raft.pb.h"
#include "Transport.h"
#include "settings/Soft.h"

namespace ycrt
{

namespace transport
{

using namespace settings;

Transport::Transport()
  : log(spdlog::get("transport")),
    streamConnections(Soft::ins().StreamConnections),
    sendQueueLength(Soft::ins().SendQueueLength),
    getConnectedTimeoutS(Soft::ins().GetConnectedTimeoutS),
    idleTimeoutS(60), // TODO: add idleTimeoutS to soft?
    errChunkSendSkipped("chunk is skipped"),
    errBatchSendSkipped("raft request batch is skipped")
{
}

} // namespace transport

} // namespace ycrt