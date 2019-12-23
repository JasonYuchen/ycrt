//
// Created by jason on 2019/12/21.
//

#include "pb/raft.pb.h"
#include "Transport.h"
#include "settings/soft.h"

namespace ycrt
{

namespace transport
{

using namespace settings;

Transport::Transport()
  : log(spdlog::get("transport")),
    streamConnections(soft::ins().StreamConnections),
    sendQueueLength(soft::ins().SendQueueLength),
    getConnectedTimeoutS(soft::ins().GetConnectedTimeoutS),
    idleTimeoutS(60), // TODO: add idleTimeoutS to soft?
    errChunkSendSkipped("chunk is skipped"),
    errBatchSendSkipped("raft request batch is skipped")
{
}

} // namespace transport

} // namespace ycrt