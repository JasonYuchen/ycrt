//
// Created by jason on 2020/1/3.
//

#ifndef YCRT_UTILS_ERROR_H_
#define YCRT_UTILS_ERROR_H_

#include <stdexcept>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include "Types.h"

namespace ycrt
{

enum class ErrorCode : uint32_t {
  OK = 0,
  FileSystem,
  InvalidConfig,
  BatchSendSkipped,
  ChunkSendSkipped,
  RemoteState,
  OutOfRange,
  LogCompacted,
  LogUnavailable,
  LogMismatch,
  SnapshotUnavailable,
  SnapshotOutOfDate,
  SnapshotEnvError,
  UnexpectedRaftState,
  UnexpectedRaftMessage,
  InvalidReadIndex,
  InvalidUpdate,
  Other,
};

// maybe recoverable
class Error : public std::runtime_error {
 public:
  explicit Error(ErrorCode code, const char *what = "")
    : std::runtime_error(what), code_(code) {}
  Error(ErrorCode code, const std::string &what)
    : std::runtime_error(what), code_(code) {}
  // format error message
  template<typename S, typename... Args>
  Error(ErrorCode code, const S& format_str, Args&&... args)
    : std::runtime_error(
        fmt::format(format_str, std::forward<Args>(args)...)),
      code_(code) {}
  // format error message and do log
  template<typename S, typename... Args>
  Error(ErrorCode code,
    const std::shared_ptr<spdlog::logger> &logger,
    const S& format_str, Args&&... args)
    : std::runtime_error(
        fmt::format(format_str, std::forward<Args>(args)...)),
      code_(code) { logger->critical(std::runtime_error::what()); }
 private:
  ErrorCode code_;
};

class Status {
 public:
  Status() : error_(ErrorCode::OK) {}
  Status(ErrorCode error) : error_(error) {}
  DEFAULT_COPY_MOVE_AND_ASSIGN(Status);

  ErrorCode Code() const { return error_; }
  bool IsOK() const { return error_ == ErrorCode::OK; }
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
  StatusWith(const Result &r, ErrorCode error = ErrorCode::OK)
    : result_(r), error_(error) {}
  StatusWith(Result &&r, ErrorCode error = ErrorCode::OK)
    : result_(std::move(r)), error_(error) {}
  StatusWith(ErrorCode error) : result_(), error_(error) {}
  StatusWith(const Status &s) : result_(), error_(s.Code()) {}
  StatusWith(Status &&s) : result_(), error_(s.Code()) {}
  DEFAULT_COPY_MOVE_AND_ASSIGN(StatusWith);

  ErrorCode Code() const { return error_; }
  bool IsOK() const { return error_ == ErrorCode::OK; }
  bool IsOKOrThrow()
  {
    if (error_ != ErrorCode::OK) {
      throw Error(error_);
    }
    return true;
  }
  const Result &GetOrThrow() const
  {
    if (!IsOK()) {
      throw Error(error_);
    }
    return result_;
  }
  Result &GetMutableOrThrow()
  {
    if (!IsOK()) {
      throw Error(error_);
    }
    return result_;
  }
  Result GetOrDefault(const Result &r)
  {
    if (error_ != ErrorCode::OK) {
      return r;
    } else {
      return result_;
    }
  }
  Result GetOrDefault(Result &&r)
  {
    if (error_ != ErrorCode::OK) {
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
