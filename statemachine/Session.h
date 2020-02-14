//
// Created by jason on 2020/2/12.
//

#ifndef YCRT_STATEMACHINE_SESSION_H_
#define YCRT_STATEMACHINE_SESSION_H_

#include <stdint.h>
#include <unordered_map>

#include "ycrt/StateMachine.h"
#include "utils/DataStructure.h"

namespace ycrt
{

namespace statemachine
{

class Session {
 public:
  explicit Session(uint64_t clientID);
  uint64_t ClientID() const;
  uint64_t RespondedUpTo() const;
  void AddResponse(uint64_t seriesID, ResultSPtr result);
  ResultSPtr GetResponse(uint64_t seriesID) const;
  bool HasResponded(uint64_t seriesID) const;
  // UpdateRespondedTo updates the responded to value of the specified
  // client session.
  void UpdateRespondedTo(uint64_t seriesID);
  // serialized bytes will be appended to buf, return number of bytes
  size_t Save(std::string &buf) const;
  // buf should be used to exactly construct Session without any trailing bytes
  size_t Load(string_view buf);
 private:
  uint64_t clientID_;
  uint64_t respondedUpTo_;
  std::unordered_map<uint64_t, ResultSPtr> history_;
};
using SessionSPtr = std::shared_ptr<Session>;

class SessionManager {
 public:
  SessionManager();
  // RegisterClientID registers a new client, it returns the input client id
  // if it is previously unknown, or 0 when the client has already been
  // registered.
  Result RegisterClientID(uint64_t clientID);
  // UnregisterClientID removes the specified client session from the system.
  // It returns the client id if the client is successfully removed, or 0
  // if the client session does not exist.
  Result UnregisterClientID(uint64_t clientID);
  // GetRegisteredClient returns the specified client if exists in the system
  // or nullptr
  SessionSPtr GetRegisteredClient(uint64_t clientID);
  size_t SaveSessions(std::string &buf);
  size_t LoadSessions(string_view buf);
 private:
  slogger log;
  std::mutex mutex_;
  LRUCache<uint64_t, SessionSPtr> sessions_; // RaftClientID -> Session
};

} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_SESSION_H_
