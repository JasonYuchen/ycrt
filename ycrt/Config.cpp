//
// Created by jason on 2019/12/24.
//

#include <settings/Soft.h>
#include "Config.h"
#include "Status.h"
#include "utils/Utils.h"

namespace ycrt
{

void Config::Validate()
{
  if (NodeID == 0) {
    throw Error(ErrorCode::InvalidConfig, "NodeID must be > 0");
  }
  if (HeartbeatRTT == 0) {
    throw Error(ErrorCode::InvalidConfig, "HeartbeatRTT must be > 0");
  }
  if (ElectionRTT <= 2 * HeartbeatRTT) {
    throw Error(ErrorCode::InvalidConfig,
      "ElectionRTT must be > 2 * HeartbeatRTT");
  }
  if (ElectionRTT < 10 * HeartbeatRTT) {
    Log.GetLogger("config")->warn(
      "ElectionRTT({0:d}) is not a magnitude larger than HeartbeatRtt({1:d})",
      ElectionRTT, HeartbeatRTT);
  }
  if (MaxInMemLogSize <= settings::EntryNonCmdSize) {
    throw Error(ErrorCode::InvalidConfig,
      "MaxInMemLogSize must be > settings::EntryNonCmdSize");
  }
  if (SnapshotCompressionType != Snappy
    && SnapshotCompressionType != NoCompression) {
    throw Error(ErrorCode::InvalidConfig,
      "SnapshotCompressionType must be Snappy or NoCompression");
  }
  if (EntryCompressionType != Snappy
    && EntryCompressionType != NoCompression) {
    throw Error(ErrorCode::InvalidConfig,
      "EntryCompressionType must be Snappy or NoCompression");
  }
  if (IsWitness && SnapshotEntries > 0) {
    throw Error(ErrorCode::InvalidConfig, "Witness node can not take snapshot");
  }
  if (IsWitness && IsObserver) {
    throw Error(ErrorCode::InvalidConfig, "Node can not be both witness and observer");
  }
}

void NodeHostConfig::Validate()
{
  if (RTTMillisecond == 0) {
    throw Error(ErrorCode::InvalidConfig, "RTTMillisecond must be > 0");
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
      throw Error(ErrorCode::InvalidConfig, "CAFile not specified");
    }
    if (CertFile.empty()) {
      throw Error(ErrorCode::InvalidConfig, "CertFile not specified");
    }
    if (KeyFile.empty()) {
      throw Error(ErrorCode::InvalidConfig, "KeyFile not specified");
    }
  }
  if (MaxSendQueueSize <= settings::EntryNonCmdSize) {
    throw Error(ErrorCode::InvalidConfig,
      "MaxSendQueueSize must be > settings::EntryNonCmdSize");
  }
  if (MaxReceiveQueueSize <= settings::EntryNonCmdSize) {
    throw Error(ErrorCode::InvalidConfig,
      "MaxReceiveQueueSize must be > settings::EntryNonCmdSize");
  }
}

} // namespace ycrt