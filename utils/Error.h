//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_UTILS_ERROR_H_
#define YCRT_UTILS_ERROR_H_

#include <stdexcept>

namespace ycrt
{

enum ErrorCode : uint32_t {
  OK = 0,
  errInvalidConfig = 1,
  errBatchSendSkipped = 2,
  errChunkSendSkipped = 3,
  errRemoteState,
};

// maybe recoverable
class Error : public std::runtime_error {
 public:
  Error(ErrorCode code, const char *what = "")
    : std::runtime_error(what), code_(code) {}
  Error(ErrorCode code, const std::string &what)
    : std::runtime_error(what), code_(code) {}
  static Error InvalidConfig(const char *what) { return Error(errInvalidConfig, what); }
 private:
  ErrorCode code_;
};

// unrecoverable fatal errors, system is inconsistent or corrupted
class Fatal : public std::runtime_error {
 public:
  Fatal(ErrorCode code, const char *what = "")
    : std::runtime_error(what), code_(code) {}
  Fatal(ErrorCode code, const std::string &what)
    : std::runtime_error(what), code_(code) {}
 private:
  ErrorCode code_;
};

} // namespace ycrt

#endif //YCRT_UTILS_ERROR_H_
