//
// Created by jason on 2020/2/12.
//

#ifndef YCRT_STATEMACHINE_SNAPSHOTTER_H_
#define YCRT_STATEMACHINE_SNAPSHOTTER_H_

#include "SnapshotIO.h"

namespace ycrt
{

namespace statemachine
{

class Manager;
class Snapshotter {
 public:
  std::string ID() const {
    return node_.fmt();
  }
  // TODO: Stream
  StatusWith<std::pair<pbSnapshotSPtr, server::SnapshotEnvUPtr>> Save(
    Manager &manager,
    SnapshotMeta &meta);
  Status Load();
 private:
  server::SnapshotEnvUPtr getSnapshotEnv(const SnapshotMeta &meta);
  server::SnapshotEnvUPtr getSnapshotEnv(uint64_t index);
  slogger log;
  NodeInfo node_;
  server::SnapshotLocator locator_;
  NodeHostConfigSPtr nhConfig_;
  boost::filesystem::path dir_;
  logdb::LogDBSPtr logDB_;
  std::atomic_bool &stopped_;
};

} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_SNAPSHOTTER_H_
