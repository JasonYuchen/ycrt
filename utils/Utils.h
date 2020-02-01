//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_UTILS_UTILS_H_
#define YCRT_UTILS_UTILS_H_

/// Libraries for reference:
// concurrent hash table: https://github.com/efficient/libcuckoo
// concurrent data structure: https://github.com/khizmax/libcds

#include <memory>
#include <experimental/string_view>
#include <iostream>
#include "Error.h"
#include "Types.h"
#include "Logger.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "Circuitbreaker.h"

namespace ycrt
{

template<typename T>
using BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;
template<typename T>
using BlockingConcurrentQueueSPtr
  = std::shared_ptr<moodycamel::BlockingConcurrentQueue<T>>;
template<typename T>
using BlockingConcurrentQueueUPtr
  = std::unique_ptr<moodycamel::BlockingConcurrentQueue<T>>;


using slogger = std::shared_ptr<spdlog::logger>;
using string_view = std::experimental::string_view;
using CircuitBreaker = circuitbreaker::NaiveCircuitBreaker;

template<typename T>
class Span {
 public:
  Span(T *start, size_t len) : start_(start), len_(len) {}
  DEFAULT_COPY_MOVE_AND_ASSIGN(Span);
  explicit Span(std::vector<T> &data) : start_(data.data()), len_(data.size()) {}
  T *begin() noexcept
  {
    return start_;
  }
  const T *begin() const noexcept
  {
    return start_;
  }
  const T *cbegin() const noexcept
  {
    return start_;
  }
  T *end() noexcept
  {
    return start_ + len_;
  }
  const T *end() const noexcept
  {
    return start_ + len_;
  }
  const T *cend() const noexcept
  {
    return start_ + len_;
  }
  size_t size() const noexcept
  {
    return len_;
  }
  bool empty() const noexcept
  {
    return len_ == 0;
  }
  T &back() noexcept
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[len_-1];
  }
  const T &back() const noexcept
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[len_-1];
  }
  T &front() noexcept
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[0];
  }
  const T &front() const noexcept
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[0];
  }
  T &operator[](size_t idx) noexcept
  {
    assert(idx < len_);
    return start_[idx];
  }
  const T &operator[](size_t idx) const noexcept
  {
    assert(idx < len_);
    return start_[idx];
  }
  T *data() noexcept
  {
    return start_;
  }
  const T *data() const noexcept
  {
    return start_;
  }
  Span SubSpan(size_t index) const
  {
    if (index > len_) {
      throw Error(ErrorCode::OutOfRange);
    }
    return Span(start_ + index, len_ - index);
  }
  Span SubSpan(size_t index, size_t len) const
  {
    if (index + len > len_) {
      throw Error(ErrorCode::OutOfRange);
    }
    return Span(start_ + index, len);
  }
  std::vector<T> ToVector() const
  {
    std::vector<T> copy;
    copy.resize(len_);
    ::memcpy(copy.data(), start_, sizeof(T) * len_);
    return copy;
  }
 private:
  T *start_;
  size_t len_;
};

class Stopper {
 public:
  Stopper() : stopped_(false), workers_() {}
  void RunWorker(std::function<void()> &&main)
  {
    workers_.emplace_back(std::thread(
      [main=std::move(main)]()
      {
        try {
          main();
        } catch (std::exception &ex) {
          std::cerr << "exception caught: " << ex.what() << std::endl;
        } catch (...) {
          std::cerr << "unknown exception" << std::endl;
        }
      }));
  }
  void Stop()
  {
    stopped_ = true;
  }
  bool ShouldStop()
  {
    return stopped_;
  }
 private:
  std::atomic_bool stopped_;
  std::vector<std::thread> workers_;
};

// for output purpose, [clusterID_:nodeID_], e.g. [00001:00005]
inline std::string FmtClusterNode(uint64_t clusterID, uint64_t nodeID)
{
  return fmt::format("[{0:05d}:{1:05d}]", clusterID, nodeID);
}

} // namespace ycrt

#endif //YCRT_UTILS_UTILS_H_
