//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_YCRT_STATUS_H_
#define YCRT_YCRT_STATUS_H_

#include <string>
#include <stdexcept>
#include "utils/Error.h"

namespace ycrt
{

class Status {
 public:
  explicit Status(ErrorCode code, const char *what = "")
    : code_(code), what_(what) {}
  explicit Status(ErrorCode code, std::string what = "")
    : code_(code), what_(std::move(what)) {}
 private:
  friend bool operator==(const Status &lhs, const Status &rhs);
  friend bool operator==(const Status &err, const ErrorCode &code);
  ErrorCode code_;
  std::string what_;
};

inline bool operator==(const Status &lhs, const Status &rhs)
{
  return lhs.code_ == rhs.code_;
}

inline bool operator==(const Status &err, const ErrorCode &code)
{
  return err.code_ == code;
}

} // namespace ycrt

#endif //YCRT_YCRT_STATUS_H_
