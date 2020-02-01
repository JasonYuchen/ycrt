//
// Created by jason on 2020/1/29.
//

#ifndef YCRT_RAFT_LOGENTRYUTILS_H_
#define YCRT_RAFT_LOGENTRYUTILS_H_

#include "pb/RaftMessage.h"
#include "utils/Error.h"

namespace ycrt
{

namespace raft
{
inline void CheckEntriesToAppend(
  const Span<pbEntry> existing,
  const Span<pbEntry> append)
{
  if (existing.empty() || append.empty()) {
    return;
  }
  if (existing.back().index() + 1 != append.front().index()) {
    throw Error(ErrorCode::LogMismatch,
      "found a hold, exist {0}, append {1}",
      existing.back().index(), append.front().index());
  }
  if (existing.back().term() > append.front().term()) {
    throw Error(ErrorCode::LogMismatch,
      "unexpected term, ecist {0}, append {1}",
      existing.back().term(), append.front().term());
  }
}
} // namespace raft

} // namespace ycrt

#endif //YCRT_RAFT_LOGENTRYUTILS_H_
