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
  T *begin()
  {
    return start_;
  }
  const T *cbegin() const
  {
    return start_;
  }
  T *end()
  {
    return start_ + len_;
  }
  const T *cend()
  {
    return start_ + len_;
  }
  size_t size()
  {
    return len_;
  }
  T &operator[](size_t idx)
  {
    assert(idx < len_);
    return start_[idx];
  }
  const T &operator[](size_t idx) const
  {
    assert(idx < len_);
    return start_[idx];
  }
  T *data()
  {
    return start_;
  }
  const T *data() const
  {
    return start_;
  }
  Span SubSpan(size_t index, size_t len)
  {
    if (index + len > len_) {
      throw Error(errOutOfRange);
    }
    return Span(start_ + index, len);
  }
 private:
  T *start_;
  size_t len_;
};

} // namespace ycrt

#endif //YCRT_UTILS_UTILS_H_
