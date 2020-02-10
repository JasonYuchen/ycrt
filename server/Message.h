//
// Created by jason on 2020/2/10.
//

#ifndef YCRT_SERVER_MESSAGE_H_
#define YCRT_SERVER_MESSAGE_H_

#include <stdint.h>
#include <assert.h>
#include <vector>
#include <mutex>
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace server
{

// TODO: rate limit, gc
template<typename T>
class DoubleBufferingQueue {
 public:
  DoubleBufferingQueue(uint64_t size)
    : mutex_(), current_(0), size_(size), buffer_()
  {
    buffer_[0].reserve(size);
    buffer_[1].reserve(size);
  }
  bool Add(const T &t) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (buffer_[current_ % 2].size() >= size_) {
      return false;
    }
    buffer_[current_ % 2].emplace_back(t);
    return true;
  }
  bool Add(T &&t) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (buffer_[current_ % 2].size() >= size_) {
      return false;
    }
    buffer_[current_ % 2].emplace_back(std::move(t));
    return true;
  }
  std::vector<T> Get()
  {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<T> result(
      std::make_move_iterator(buffer_[current_ % 2].begin()),
      std::make_move_iterator(buffer_[current_ % 2].end()));
    buffer_[current_ % 2].clear();
    current_++;
    assert(buffer_[current_ % 2].empty());
    return result;
  }
 private:
  std::mutex mutex_;
  uint64_t current_;
  uint64_t size_;
  std::vector<T> buffer_[2];
};

using RaftMessageQueueSPtr =
  std::shared_ptr<DoubleBufferingQueue<pbMessageSPtr>>;

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_MESSAGE_H_
