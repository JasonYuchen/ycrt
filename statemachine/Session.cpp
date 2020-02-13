//
// Created by jason on 2020/2/12.
//

#include <settings/Hard.h>
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

uint64_t Session::ClientID() const
{
  return clientID_;
}

uint64_t Session::RespondedUpTo() const
{
  return respondedUpTo_;
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

void Session::UpdateRespondedTo(uint64_t seriesID)
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
  written += sizeof(size_t);
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
  uint64_t seriesID;
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

SessionManager::SessionManager()
  : log(Log.GetLogger("statemachine")),
    mutex_(),
    sessions_(settings::Hard::ins().LRUMaxSessionCount,
      [log=log](const uint64_t &clientID, SessionSPtr &session)
      {
        log->warn("SessionManager: session with clientID={} evicted", clientID);
      })
{
}

Result SessionManager::RegisterClientID(uint64_t clientID)
{
  SessionSPtr session;
  bool done = false;
  {
    lock_guard<mutex> guard(mutex_);
    done = sessions_.Get(clientID, session);
  }
  if (done) {
    if (session->ClientID() != clientID) {
      throw Error(ErrorCode::InvalidSession, log,
        "SessionManager::RegisterClientID: unexpected existing session with "
        "clientID={}, expected={}", session->ClientID(), clientID);
    }
    log->warn("SessionManager::RegisterClientID: session with clientID={} "
              "already exists", clientID);
    return {};
  }
  {
    lock_guard<mutex> guard(mutex_);
    sessions_.Put(clientID, make_shared<Session>(clientID));
  }
  Result result;
  result.Value = clientID;
  return result;
}

Result SessionManager::UnregisterClientID(uint64_t clientID)
{
  SessionSPtr session;
  bool done = false;
  {
    lock_guard<mutex> guard(mutex_);
    done = sessions_.Get(clientID, session);
  }
  if (!done) {
    return {};
  }
  if (session->ClientID() != clientID) {
    throw Error(ErrorCode::InvalidSession, log,
      "SessionManager::UnregisterClientID: unexpected existing session with "
      "clientID={}, expected={}", session->ClientID(), clientID);
  }
  {
    lock_guard<mutex> guard(mutex_);
    sessions_.Del(clientID);
  }
  Result result;
  result.Value = clientID;
  return result;
}

SessionSPtr SessionManager::GetRegisteredClient(uint64_t clientID)
{
  SessionSPtr session;
  {
    lock_guard<mutex> guard(mutex_);
    sessions_.Get(clientID, session);
  }
  return session;
}

uint64_t SessionManager::SaveSessions(std::string &buf)
{
  // TODO
  return 0;
}

uint64_t SessionManager::LoadSessions(ycrt::string_view buf)
{
  // TODO
  return 0;
}

} // namespace statemachine

} // namespace ycrt