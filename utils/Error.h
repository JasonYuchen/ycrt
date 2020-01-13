//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_UTILS_ERROR_H_
#define YCRT_UTILS_ERROR_H_

#include <stdexcept>
#include <spdlog/fmt/fmt.h>
#include "Types.h"

namespace ycrt
{

enum ErrorCode : uint32_t {
  OK = 0,
  errInvalidConfig,
  errBatchSendSkipped,
  errChunkSendSkipped,
  errRemoteState,
  errOutOfRange,
  errLogCompacted,
  errLogUnavailable,
  errSnapshotUnavailable,
  errUnexpectedRaftState,
  errRaftMessage,
  errLogMismatch,
  errEmptySnapshot,
  errInvalidReadIndex,
  errOther
};

// maybe recoverable
class Error : public std::runtime_error {
 public:
  explicit Error(ErrorCode code, const char *what = "")
    : std::runtime_error(what), code_(code) {}
  Error(ErrorCode code, const std::string &what)
    : std::runtime_error(what), code_(code) {}
  template<typename S, typename... Args>
  Error(ErrorCode code, const S& format_str, Args&&... args)
    : std::runtime_error(
        fmt::format(format_str, std::forward<Args>(args)...)),
      code_(code) {}
 private:
  ErrorCode code_;
};

class Status {
 public:
  Status() : error_(OK) {}
  explicit Status(ErrorCode error) : error_(error) {}
  DEFAULT_COPY_MOVE_AND_ASSIGN(Status);

  ErrorCode Code() const { return error_; }
  bool IsOK() const { return error_ == OK; }
 private:
  ErrorCode error_;
};

inline bool operator==(const Status &lhs, Status rhs)
{
  return lhs.Code() == rhs.Code();
}

inline bool operator==(const Status &lhs, ErrorCode rhs)
{
  return lhs.Code() == rhs;
}

template<typename Result>
class StatusWith {
 public:
  explicit StatusWith(const Result &r) : result_(r), error_(OK) {}
  explicit StatusWith(Result &&r) : result_(std::move(r)), error_(OK) {}
  explicit StatusWith(ErrorCode error) : result_(), error_(error) {}
  DEFAULT_COPY_MOVE_AND_ASSIGN(StatusWith);

  ErrorCode Code() const { return error_; }
  bool IsOK() const { return error_ == OK; }
  bool IsOKOrThrow()
  {
    if (error_ != OK) {
      throw Error(error_);
    }
    return true;
  }
  const Result &Get() const { return result_; }
  Result &GetMutable() { return result_; }
  const Result &GetOrThrow() const
  {
    if (error_ != OK) {
      throw Error(error_);
    }
    return result_;
  }
  Result &GetMutableOrThrow()
  {
    if (error_ != OK) {
      throw Error(error_);
    }
    return result_;
  }
  Result GetOrDefault(const Result &r)
  {
    if (error_ != OK) {
      return r;
    } else {
      return result_;
    }
  }
  Result GetOrDefault(Result &&r)
  {
    if (error_ != OK) {
      return r;
    } else {
      return result_;
    }
  }
private:
  // use unique_ptr to store the result?
  Result result_;
  ErrorCode error_;
};

} // namespace ycrt

#endif //YCRT_UTILS_ERROR_H_
