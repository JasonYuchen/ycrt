//
// Created by jason on 2020/1/12.
//

#ifndef YCRT_RAFT_READINDEX_H_
#define YCRT_RAFT_READINDEX_H_

#include <stdint.h>
#include <iterator>
#include <set>

#include "utils/Error.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace raft
{

struct ReadStatus {
  ReadStatus(uint64_t index, uint64_t from, pbReadIndexCtx ctx)
    : Index(index), From(from), Ctx(ctx), Confirmed() {}
  uint64_t Index;
  uint64_t From;
  pbReadIndexCtx Ctx;
  std::set<uint64_t> Confirmed;
};

class ReadIndex {
 public:
  ReadIndex() = default;
  DEFAULT_COPY_AND_ASSIGN(ReadIndex);

  void AddRequest(uint64_t index, pbReadIndexCtx ctx, uint64_t from)
  {
    if (pending_.find(ctx) != pending_.end()) {
      return;
    }
    // index is the committed value of the cluster, it should never move
    // backward, check it here
    if (!queue_.empty()) {
      auto it = pending_.find(queue_.back());
      if (it == pending_.end()) {
        throw Error(ErrorCode::InvalidReadIndex,
          "inconsistent pending and queue");
      }
      if (index < it->second.Index) {
        throw Error(ErrorCode::InvalidReadIndex,
          "index moved backward in ReadIndex {}:{}", index, it->second.Index);
      }
    }
    queue_.emplace_back(ctx);
    pending_.insert({ctx, ReadStatus{index, from, ctx}});
  }
  std::vector<ReadStatus> Confirm(pbReadIndexCtx ctx, uint64_t from, uint64_t quorum)
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
        throw Error(ErrorCode::InvalidReadIndex,
          "inconsistent pending and queue");
      }
      confirm.emplace_back(status->second);
      if (qCtx == ctx) {
        for (auto &cCtx : confirm) {
          if (cCtx.Index > status->second.Index) {
            throw Error(ErrorCode::InvalidReadIndex, "unexpected index");
          }
          cCtx.Index = status->second.Index;
        }
        queue_ = std::vector<pbReadIndexCtx>(queue_.begin() + done, queue_.end());
        for (auto &cCtx : confirm) {
          pending_.erase(cCtx.Ctx);
        }
        if (queue_.size() != pending_.size()) {
          throw Error(ErrorCode::InvalidReadIndex,
            "inconsistent pending and queue");
        }
        return confirm;
      }
    }
    return {};
  }
  bool HasPendingRequest() const
  {
    return !queue_.empty();
  }
  pbReadIndexCtx BackCtx() const
  {
    return queue_.back();
  }
  void Clear()
  {
    pending_.clear();
    queue_.clear();
  }
 private:
  std::unordered_map<pbReadIndexCtx, ReadStatus, ReadIndexCtxHash> pending_;
  std::vector<pbReadIndexCtx> queue_;
};

} // namespace raft

} // namespace ycrt


#endif //YCRT_RAFT_READINDEX_H_
