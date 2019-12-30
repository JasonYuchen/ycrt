//
// Created by jason on 2019/12/21.
//

#include "pb/raft.pb.h"
#include "Transport.h"
#include "settings/Soft.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace transport
{

using namespace settings;

Transport::Transport()
  : log(Log.get("transport")),
    streamConnections_(Soft::ins().StreamConnections),
    sendQueueLength_(Soft::ins().SendQueueLength),
    getConnectedTimeoutS_(Soft::ins().GetConnectedTimeoutS),
    idleTimeoutS_(60), // TODO: add idleTimeoutS to soft?
{
}

} // namespace transport

} // namespace ycrt