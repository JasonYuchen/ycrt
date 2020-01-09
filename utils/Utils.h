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

} // namespace ycrt

#endif //YCRT_UTILS_UTILS_H_
