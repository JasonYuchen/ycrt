//
// Created by jason on 2020/2/1.
//

#include "LogReader.h"

#include <memory>

namespace ycrt
{

namespace logdb
{

using namespace std;

unique_ptr<LogReader> LogReader::New(NodeInfo node, LogDBSPtr logdb)
{
  unique_ptr<LogReader> reader(new LogReader(node, std::move(logdb)));
  return reader;
}

pair<uint64_t, uint64_t> LogReader::GetRange()
{
  lock_guard<mutex> guard(mutex_);
  return {firstIndex(), lastIndex()};
}

void LogReader::SetRange(uint64_t index, uint64_t length)
{
  if (length == 0) {
    return;
  }
  lock_guard<mutex> guard(mutex_);
  uint64_t curfirst = firstIndex();
  uint64_t setlast = index + length - 1;
  // current range includes the [index, index+length)
  if (setlast < curfirst) {
    return;
  }
  // overlap
  if (curfirst > index) {
    length -= curfirst - index;
    index = curfirst;
  }
  uint64_t offset = index - markerIndex_;
  if (length_ > offset) {
    length_ = offset + length;
  } else if (length_ == offset) {
    length_ += length;
  } else {
    // curfirst < index, log hole found
    throw Error(ErrorCode::LogMismatch, log,
      "LogReader::SetRange: hole found, current first={} < set={}",
      curfirst, index);
  }
}

pbState LogReader::GetNodeState()
{
  lock_guard<mutex> guard(mutex_);
  return state_;
}

void LogReader::SetNodeState(const pbState &state)
{
  lock_guard<mutex> gurad(mutex_);
  state_ = state;
}

pbSnapshotSPtr LogReader::GetSnapshot()
{
  lock_guard<mutex> guard(mutex_);
  return snapshot_;
}

Status LogReader::SetSnapshot(pbSnapshotSPtr s)
{
  lock_guard<mutex> guard(mutex_);
  if (snapshot_->index() >= s->index()) {
    return ErrorCode::SnapshotOutOfDate;
  }
  snapshot_ = std::move(s);
  return ErrorCode::OK;
}

Status LogReader::ApplySnapshot(pbSnapshotSPtr s)
{
  lock_guard<mutex> guard(mutex_);
  if (snapshot_->index() >= s->index()) {
    return ErrorCode::SnapshotOutOfDate;
  }
  markerIndex_ = s->index();
  markerTerm_ = s->term();
  length_ = 1;
  snapshot_ = std::move(s);
  return ErrorCode::OK;
}

StatusWith<uint64_t> LogReader::Term(uint64_t index)
{
  lock_guard<mutex> guard(mutex_);
  return term(index);
}

Status LogReader::GetEntries(
  EntryVector &entries,
  uint64_t low,
  uint64_t high,
  uint64_t maxSize)
{
  assert(entries.empty());
  if (low > high) {
    return ErrorCode::OutOfRange;
  }
  lock_guard<mutex> guard(mutex_);
  return getEntries(entries, low, high, maxSize);
}

Status LogReader::Compact(uint64_t index)
{
  lock_guard<mutex> guard(mutex_);
  if (index < markerIndex_) {
    return ErrorCode::LogCompacted;
  }
  if (index > lastIndex()) {
    return ErrorCode::LogUnavailable;
  }
  auto _term = term(index);
  if (!_term.IsOK()) {
    return _term.Code();
  }
  length_ -= index - markerIndex_;
  markerIndex_ = index;
  markerTerm_ = _term.GetOrThrow();
  return ErrorCode::OK;
}
Status LogReader::Append(Span<pbEntry> entries)
{
  if (entries.empty()) {
    return ErrorCode::OK;
  }
  if (entries.front().index()+entries.size()-1 != entries.back().index()) {
    throw Error(ErrorCode::LogMismatch, log, "LogReader::Append: gap found");
  }
  SetRange(entries.front().index(), entries.size());
  return ErrorCode::OK;
}

LogReader::LogReader(NodeInfo node, LogDBSPtr logdb)
  : mutex_(),
    node_(node),
    nodeDesc_(node.fmt()),
    logdb_(std::move(logdb)),
    state_(),
    snapshot_(),
    markerIndex_(0),
    markerTerm_(0),
    length_(1)
{}



} // namespace logdb

} // namespace ycrt


