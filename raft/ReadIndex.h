//
// Created by jason on 2020/1/12.
//

#ifndef YCRT_RAFT_READINDEX_H_
#define YCRT_RAFT_READINDEX_H_

#include <stdint.h>
#include <iterator>

#include "utils/Error.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace raft
{

struct ReadStatus {
  ReadStatus(uint64_t index, uint64_t from, pbSystemCtx ctx)
    : Index(index), From(from), Ctx(ctx), Confirmed() {}
  uint64_t Index;
  uint64_t From;
  pbSystemCtx Ctx;
  std::set<uint64_t> Confirmed;
};

class ReadIndex {
 public:
  ReadIndex() = default;
  DEFAULT_COPY_AND_ASSIGN(ReadIndex);

  void AddRequest(uint64_t index, pbSystemCtx ctx, uint64_t from)
  {
    if (pending_.find(ctx) != pending_.end()) {
      return;
    }
    // index is the committed value of the cluster, it should never move
    // backward, check it here
    if (!queue_.empty()) {
      auto it = pending_.find(queue_.back());
      if (it == pending_.end()) {
        throw Error(errInvalidReadIndex, "inconsistent pending and queue");
      }
      if (index < it->second.Index) {
        throw Error(errInvalidReadIndex, "index moved backward in ReadIndex {0}:{1}", index, it->second.Index);
      }
    }
    queue_.emplace_back(ctx);
    pending_.insert({ctx, ReadStatus{index, from, ctx}});
  }
  std::vector<ReadStatus> Confirm(pbSystemCtx ctx, uint64_t from, uint64_t quorum)
  {
    auto it = pending_.find(ctx);
    if (it == pending_.end()) {
      return {};
    }
    it->second.Confirmed.insert(from);
    if (it->second.Confirmed.size() + 1 < quorum) {
      return {};
    }
    uint64_t done = 0;
    std::vector<ReadStatus> confirm;
    for (auto &qCtx : queue_) {
      done++;
      auto status = pending_.find(qCtx);
      if (status == pending_.end()) {
        throw Error(errInvalidReadIndex, "inconsistent pending and queue");
      }
      confirm.emplace_back(status->second);
      if (qCtx == ctx) {
        for (auto &cCtx : confirm) {
          if (cCtx.Index > status->second.Index) {
            throw Error(errInvalidReadIndex, "unexpected index");
          }
          cCtx.Index = status->second.Index;
        }
        queue_ = std::vector<pbSystemCtx>(queue_.begin() + done, queue_.end());
        for (auto &cCtx : confirm) {
          pending_.erase(cCtx.Ctx);
        }
        if (queue_.size() != pending_.size()) {
          throw Error(errInvalidReadIndex, "inconsistent pending and queue");
        }
        return confirm;
      }
    }
  }
  bool HasPendingRequest() const
  {
    return !queue_.empty();
  }
  pbSystemCtx BackCtx() const
  {
    return queue_.back();
  }
  void Clear()
  {
    pending_.clear();
    queue_.clear();
  }
 private:
  std::unordered_map<pbSystemCtx, ReadStatus, SystemCtxHash> pending_;
  std::vector<pbSystemCtx> queue_;
};

} // namespace raft

} // namespace ycrt


#endif //YCRT_RAFT_READINDEX_H_
