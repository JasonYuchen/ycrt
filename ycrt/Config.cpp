//
// Created by jason on 2019/12/24.
//

#include "Config.h"
#include "Error.h"
#include "utils/Logger.h"

namespace ycrt
{

void Config::validate()
{
  if (NodeID == 0) {
    throw Error("NodeID must be > 0");
  }
  if (HeartbeatRTT == 0) {
    throw Error("HeartbeatRTT must be > 0");
  }
  if (ElectionRTT <= 2 * HeartbeatRTT) {
    throw Error("ElectionRTT must be > 2 * HeartbeatRTT");
  }
  if (ElectionRTT < 10 * HeartbeatRTT) {
    Log.get("config")->warn(
      "ElectionRTT({0:d}) is not a magnitude larger than HeartbeatRtt({1:d})",
      ElectionRTT, HeartbeatRTT);
  }
}

} // namespace ycrt