//
// Created by jason on 2020/2/12.
//

#include "Session.h"

namespace ycrt
{

namespace statemachine
{

using namespace std;

Session::Session(uint64_t clientID)
  : clientID_(clientID), respondedUpTo_(0), history_()
{
}

void Session::AddResponse(uint64_t seriesID, ResultSPtr result)
{
  auto item = history_.find(seriesID);
  if (item != history_.end()) {
    throw Error(ErrorCode::AlreadyExists, "adding a duplicated response");
  }
  history_[seriesID] = std::move(result);
}

ResultSPtr Session::GetResponse(uint64_t seriesID) const
{
  auto item = history_.find(seriesID);
  if (item == history_.end()) {
    return nullptr;
  }
  return item->second;
}

void Session::ClearTo(uint64_t seriesID)
{
  if (seriesID <= respondedUpTo_) {
    return;
  }
  respondedUpTo_ = seriesID;
  if (seriesID == respondedUpTo_ + 1) {
    history_.erase(seriesID);
    return;
  }
  for (auto it = history_.begin(); it != history_.end();) {
    if (it->first <= seriesID) {
      it = history_.erase(it);
    } else {
      it++;
    }
  }
}

bool Session::HasResponded(uint64_t seriesID) const
{
  return seriesID <= respondedUpTo_;
}

size_t Session::Save(string &buf) const
{
  size_t written = sizeof(clientID_);
  buf.append(reinterpret_cast<const char *>(&clientID_), sizeof(clientID_));
  written += sizeof(respondedUpTo_);
  buf.append(reinterpret_cast<const char *>(&respondedUpTo_), sizeof(respondedUpTo_));
  for (auto &resp : history_) {
    assert(resp.second);
    written += sizeof(resp.first);
    buf.append(reinterpret_cast<const char *>(&resp.first), sizeof(resp.first));
    written += resp.second->AppendToString(buf);
  }
  return written;
}

void Session::Load(string_view buf)
{
  size_t remaining = buf.size();
  if (remaining < sizeof(clientID_) + sizeof(respondedUpTo_)) {
    throw Error(ErrorCode::InvalidSession, "Session::Load: too short");
  }
  const char *cur = buf.data();
  ::memcpy(&clientID_, cur, sizeof(clientID_));
  cur += sizeof(clientID_);
  ::memcpy(&respondedUpTo_, cur, sizeof(respondedUpTo_));
  cur += sizeof(respondedUpTo_);
  uint64_t seriesID, size;
  while (remaining > 0) {
    remaining = buf.data() + buf.size() - cur;
    if (remaining < sizeof(seriesID)) {
      break;
    }
    ::memcpy(&seriesID, cur, sizeof(seriesID));
    cur += sizeof(seriesID);
    auto result = make_shared<Result>();
    cur += result->FromString({cur, remaining});
    history_[seriesID] = std::move(result);
  }
  if (remaining) {
    throw Error(ErrorCode::InvalidSession, "Session::Load: history corrupted");
  }
}

} // namespace statemachine

} // namespace ycrt
