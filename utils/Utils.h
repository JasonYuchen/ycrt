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
#include <experimental/any>
#include <iostream>
#include "Error.h"
#include "Types.h"
#include "Logger.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "CircuitBreaker.h"

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
using any = std::experimental::any;
using std::experimental::any_cast;
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
  T &back()
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[len_-1];
  }
  const T &back() const
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[len_-1];
  }
  T &front()
  {
    if (empty()) {
      throw Error(ErrorCode::OutOfRange);
    }
    return start_[0];
  }
  const T &front() const
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
  explicit Stopper(std::string name = "unknown")
    : name_(std::move(name)), stopped_(false), workers_() {}
  // not thread safe
  void RunWorker(std::function<void(std::atomic_bool&)> &&main)
  {
    if (stopped_) {
      return;
    }
    workers_.emplace_back(std::thread(
      [this, main=std::move(main)]()
      {
        try {
          main(stopped_);
        } catch (std::exception &ex) {
          std::cerr << "exception caught: " << ex.what() << std::endl;
        } catch (...) {
          std::cerr << "unknown exception" << std::endl;
        }
      }));
  }
  // not thread safe
  uint64_t WorkerCount() const
  {
    return workers_.size();
  }
  void Stop()
  {
    if (!stopped_.exchange(true)) {
      for (auto &t : workers_) {
        t.join();
      }
    }
  }
  ~Stopper()
  {
    Stop();
  }
 private:
  const std::string name_;
  std::atomic_bool stopped_;
  std::vector<std::thread> workers_;
};

struct NodeInfo {
  uint64_t ClusterID = 0;
  uint64_t NodeID = 0;
  // for output purpose, [clusterID_:nodeID_], e.g. [00001:00005]
  bool Valid() const {
    return ClusterID && NodeID;
  }
  std::string fmt() const
  {
    return fmt::format("[{0:05d}:{1:05d}]", ClusterID, NodeID);
  }
};

template<typename os>
os &operator<<(os &o, const NodeInfo &n)
{
  return o << fmt::format("[{0:05d}:{1:05d}]", n.ClusterID, n.NodeID);
}

inline bool operator==(const NodeInfo &lhs, const NodeInfo &rhs)
{
  return lhs.NodeID == rhs.NodeID && lhs.ClusterID == rhs.ClusterID;
}

struct NodeInfoHash {
  size_t operator()(const NodeInfo& rhs) const {
    return std::hash<uint64_t>()(rhs.ClusterID)
      ^ std::hash<uint64_t>()(rhs.NodeID);
  }
};

// for spdlog

} // namespace ycrt

#endif //YCRT_UTILS_UTILS_H_
