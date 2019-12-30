//
// Created by jason on 2019/12/30.
//

#ifndef YCRT_PB_RAFTMESSAGE_H_
#define YCRT_PB_RAFTMESSAGE_H_

#include "raft.pb.h"

namespace ycrt
{

using MessageBatchSPtr = std::shared_ptr<raftpb::MessageBatch>;
using MessageBatchUPtr = std::unique_ptr<raftpb::MessageBatch>;
using MessageSPtr = std::shared_ptr<raftpb::Message>;
using MessageUPtr = std::unique_ptr<raftpb::Message>;

} // namespace ycrt

#endif //YCRT_PB_RAFTMESSAGE_H_
