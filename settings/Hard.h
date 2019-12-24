//
// Created by jason on 2019/12/23.
//

#ifndef YCRT_SETTINGS_HARD_H_
#define YCRT_SETTINGS_HARD_H_

#include <stdint.h>

namespace ycrt
{

namespace settings
{

struct Hard {
  // StepEngineWorkerCount defines number of workers to use to process raft node
  // changes. Together with the LogDBPoolSize parameters below, they determine
  // the content of each logdb shards. You will have to build your own tools to
  // move logdb data around to be able to change StepEngineWorkerCount and
  // LogDBPoolSize after your system is deployed.
  uint64_t StepEngineWorkerCount = 16;
  // LogDBPoolSize defines the number of logdb shards to use. When you get slow
  // performance when using the default LogDBPoolSize value, it typically means
  // your disk is not good enough for concurrent write acdesses.
  uint64_t LogDBPoolSize = 16;
  // LRUMaxSessionCount is the max number of client sessions that can be
  // concurrently held and managed by each raft cluster.
  uint64_t LRUMaxSessionCount = 16;
  // LogDBEntryBatchSize is the max size of each entry batch.
  uint64_t LogDBEntryBatchSize = 16;

  static Hard &ins();
};

} // namespace settings

} // namespace ycrt

#endif //YCRT_SETTINGS_HARD_H_
