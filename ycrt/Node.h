//
// Created by jason on 2020/1/14.
//

#ifndef YCRT_YCRT_NODE_H_
#define YCRT_YCRT_NODE_H_

#include <stdint.h>
#include <string>

#include "Config.h"
#include "server/Message.h"

namespace ycrt
{

class Node {
 public:
 private:

  const uint64_t incomingProposalsMaxLen_;
  const uint64_t incomingReadIndexMaxLen_;
  const uint64_t syncTaskInterval_;
  const uint64_t lazyFreeCycle_;

  uint64_t readRequestCount_;
  uint64_t leaderID_;
  uint64_t instanceID_;
  std::string raftAddress_;
  Config config_;
  BlockingConcurrentQueueSPtr<pbConfigChangeSPtr> confChangeQueue_;
  // TODO: BlockingConcurrentQueueSPtr<SnapshotRequestSPtr> snapshotQueue_;
  server::RaftMessageQueueSPtr mq_;
};

} // namespace ycrt

#endif //YCRT_YCRT_NODE_H_
