//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_UTILS_UTILS_H_
#define YCRT_UTILS_UTILS_H_

/// Libraries for reference:
// concurrent hash table: https://github.com/efficient/libcuckoo
// concurrent data structure: https://github.com/khizmax/libcds

#include <memory>
#include "Logger.h"
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace ycrt
{

template<typename T>
using BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;
template<typename T>
using BlockingConcurrentQueueSPtr = std::shared_ptr<moodycamel::BlockingConcurrentQueue<T>>;
template<typename T>
using BlockingConcurrentQueueUPtr = std::unique_ptr<moodycamel::BlockingConcurrentQueue<T>>;


using slogger = std::shared_ptr<spdlog::logger>;

} // namespace ycrt

#endif //YCRT_UTILS_UTILS_H_