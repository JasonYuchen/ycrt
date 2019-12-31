//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_UTILS_ERROR_H_
#define YCRT_UTILS_ERROR_H_

#include <string>
#include <stdexcept>

namespace ycrt
{

enum ErrorCode : int32_t {
  errInvalidConfig = 1,
  errBatchSendSkipped = 2,
  errChunkSendSkipped = 3,
};

class Error : public std::runtime_error {
 public:
  explicit Error(ErrorCode code, const char *what = "")
    : std::runtime_error(what), code_(code) {}
  explicit Error(ErrorCode code, const std::string &what = "")
    : std::runtime_error(what), code_(code) {}
 private:
  friend bool operator==(const Error &lhs, const Error &rhs);
  friend bool operator==(const Error &err, const ErrorCode &code);
  ErrorCode code_;
};

inline bool operator==(const Error &lhs, const Error &rhs)
{
  return lhs.code_ == rhs.code_;
}

inline bool operator==(const Error &err, const ErrorCode &code)
{
  return err.code_ == code;
}

} // namespace ycrt

#endif //YCRT_UTILS_ERROR_H_