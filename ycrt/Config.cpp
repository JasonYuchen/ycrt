//
// Created by jason on 2019/12/24.
//

#include <settings/Soft.h>
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
  if (MaxInMemLogSize <= settings::EntryNonCmdSize) {
    throw Error("MaxInMemLogSize must be > settings::EntryNonCmdSize");
  }
  if (SnapshotCompressionType != Snappy
    && SnapshotCompressionType != NoCompression) {
    throw Error("SnapshotCompressionType must be Snappy or NoCompression");
  }
  if (EntryCompressionType != Snappy
    && EntryCompressionType != NoCompression) {
    throw Error("EntryCompressionType must be Snappy or NoCompression");
  }
  if (IsWitness && SnapshotEntries > 0) {
    throw Error("Witness node can not take snapshot");
  }
  if (IsWitness && IsObserver) {
    throw Error("Node can not be both witness and observer");
  }
}

void NodeHostConfig::validate()
{
  if (RTTMillisecond == 0) {
    throw Error("RTTMillisecond must be > 0");
  }
// FIXME
//  if (!isValidAddress(RaftAddress)) {
//    throw Error("Invalid RaftAddress");
//  }
// FIXME
//  if (!ListenAddress.empty() && !isValidAddress(ListenAddress)) {
//    throw Error("Invalid ListenAddress");
//  }
  if (ListenAddress.empty()) {
    ListenAddress = RaftAddress;
  }
  if (!MutualTLS
    && (!CAFile.empty() || !CertFile.empty() || !KeyFile.empty())) {
    Log.get("config")->warn(
      "CAFile/CertFile/KeyFile specified when MutualTLS is disabled");
  }
  if (MutualTLS) {
    if (CAFile.empty()) {
      throw Error("CAFile not specified");
    }
    if (CertFile.empty()) {
      throw Error("CertFile not specified");
    }
    if (KeyFile.empty()) {
      throw Error("KeyFile not specified");
    }
  }
  if (MaxSendQueueSize <= settings::EntryNonCmdSize) {
    throw Error("MaxSendQueueSize must be > settings::EntryNonCmdSize");
  }
  if (MaxReceiveQueueSize <= settings::EntryNonCmdSize) {
    throw Error("MaxReceiveQueueSize must be > settings::EntryNonCmdSize");
  }
}

} // namespace ycrt