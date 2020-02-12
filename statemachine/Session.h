//
// Created by jason on 2020/2/12.
//

#ifndef YCRT_STATEMACHINE_SESSION_H_
#define YCRT_STATEMACHINE_SESSION_H_

#include <stdint.h>
#include <unordered_map>

#include "ycrt/StateMachine.h"

namespace ycrt
{

namespace statemachine
{

class Session {
 public:
  explicit Session(uint64_t clientID);
  void AddResponse(uint64_t seriesID, ResultSPtr result);
  ResultSPtr GetResponse(uint64_t seriesID) const;
  bool HasResponded(uint64_t seriesID) const;
  void ClearTo(uint64_t seriesID);
  // serialized bytes will be appended to buf, return number of bytes
  size_t Save(std::string &buf) const;
  // buf should be used to exactly construct Session without any trailing bytes
  void Load(string_view buf);
 private:
  uint64_t clientID_;
  uint64_t respondedUpTo_;
  std::unordered_map<uint64_t, ResultSPtr> history_;
};

} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_SESSION_H_
