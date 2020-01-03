//
// Created by jason on 2019/12/24.
//

#include <settings/Soft.h>
#include "Config.h"
#include "Status.h"
#include "utils/Logger.h"

namespace ycrt
{

void Config::Validate()
{
  if (NodeID == 0) {
    throw Status(errInvalidConfig, "NodeID must be > 0");
  }
  if (HeartbeatRTT == 0) {
    throw Status(errInvalidConfig, "HeartbeatRTT must be > 0");
  }
  if (ElectionRTT <= 2 * HeartbeatRTT) {
    throw Status(errInvalidConfig, "ElectionRTT must be > 2 * HeartbeatRTT");
  }
  if (ElectionRTT < 10 * HeartbeatRTT) {
    Log.GetLogger("config")->warn(
      "ElectionRTT({0:d}) is not a magnitude larger than HeartbeatRtt({1:d})",
      ElectionRTT, HeartbeatRTT);
  }
  if (MaxInMemLogSize <= settings::EntryNonCmdSize) {
    throw Status(errInvalidConfig,
      "MaxInMemLogSize must be > settings::EntryNonCmdSize");
  }
  if (SnapshotCompressionType != Snappy
    && SnapshotCompressionType != NoCompression) {
    throw Status(errInvalidConfig,
      "SnapshotCompressionType must be Snappy or NoCompression");
  }
  if (EntryCompressionType != Snappy
    && EntryCompressionType != NoCompression) {
    throw Status(errInvalidConfig,
      "EntryCompressionType must be Snappy or NoCompression");
  }
  if (IsWitness && SnapshotEntries > 0) {
    throw Status(errInvalidConfig, "Witness node can not take snapshot");
  }
  if (IsWitness && IsObserver) {
    throw Status(errInvalidConfig, "Node can not be both witness and observer");
  }
}

void NodeHostConfig::Validate()
{
  if (RTTMillisecond == 0) {
    throw Status(errInvalidConfig, "RTTMillisecond must be > 0");
  }
// FIXME
//  if (!isValidAddress(RaftAddress)) {
//    throw Status("Invalid RaftAddress");
//  }
// FIXME
//  if (!ListenAddress.empty() && !isValidAddress(ListenAddress)) {
//    throw Status("Invalid ListenAddress");
//  }
  if (ListenAddress.empty()) {
    ListenAddress = RaftAddress;
  }
  if (!MutualTLS
    && (!CAFile.empty() || !CertFile.empty() || !KeyFile.empty())) {
    Log.GetLogger("config")->warn(
      "CAFile/CertFile/KeyFile specified when MutualTLS is disabled");
  }
  if (MutualTLS) {
    if (CAFile.empty()) {
      throw Status(errInvalidConfig, "CAFile not specified");
    }
    if (CertFile.empty()) {
      throw Status(errInvalidConfig, "CertFile not specified");
    }
    if (KeyFile.empty()) {
      throw Status(errInvalidConfig, "KeyFile not specified");
    }
  }
  if (MaxSendQueueSize <= settings::EntryNonCmdSize) {
    throw Status(errInvalidConfig,
      "MaxSendQueueSize must be > settings::EntryNonCmdSize");
  }
  if (MaxReceiveQueueSize <= settings::EntryNonCmdSize) {
    throw Status(errInvalidConfig,
      "MaxReceiveQueueSize must be > settings::EntryNonCmdSize");
  }
}

} // namespace ycrt