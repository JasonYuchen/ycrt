//
// Created by jason on 2020/1/9.
//

#ifndef YCRT_UTILS_CIRCUITBREAKER_CIRCUITBREAKER_H_
#define YCRT_UTILS_CIRCUITBREAKER_CIRCUITBREAKER_H_

#include <stdint.h>
#include <chrono>
#include <vector>
#include <mutex>
#include <functional>
#include <atomic>

namespace ycrt
{

namespace circuitbreaker
{

class Window {
 public:
  static constexpr uint64_t defaultTimeMS = 10000;
  static constexpr uint64_t defaultBuckets = 10;
  Window() : Window(defaultTimeMS, defaultBuckets) {}
  Window(uint64_t windowTimeMS, uint64_t windowBuckets);

  void Fail();
  void Success();
  uint64_t Failures();
  uint64_t Successes();
  void Reset();
 private:
  struct bucket {
    uint64_t failure_ = 0;
    uint64_t success_ = 0;
    inline void reset() { failure_ = 0; success_ = 0; }
    inline void fail() { failure_++; }
    inline void success() { success_++; }
  };
  bucket *getLatestBucket();

  std::vector<bucket> buckets_;
  uint64_t idx_; // ring index for buckets_
  std::chrono::milliseconds bucketTime_;
  std::mutex bucketLock_;
  std::chrono::steady_clock::time_point lastAccess_;
};

class NaiveCircuitBreaker {
 public:
  enum State { OPEN, HALFOPEN, CLOSED };
  static constexpr uint64_t defaultMultiplier = 2;
  static constexpr uint64_t defaultInitialIntervalMS = 500;
  static constexpr uint64_t defaultTimeoutIntervalMS = 100000;
  static constexpr std::chrono::milliseconds Stop{-1};
  // naive circuit breaker with an exponential backoff and no TripFunc
  NaiveCircuitBreaker();
  bool Ready();
  void Success();
  uint64_t Successes();
  void Fail();
  uint64_t Failures();
  uint64_t ConsecFailures();
  void Reset();
  void Trip();
  bool Tripped();
 private:
  State state();
  void resetBackOff();
  void setNextBackOff();
  std::function<bool(NaiveCircuitBreaker*)> shouldTrip_;
  std::atomic_uint64_t consecFailures_;
  Window counts_;
  std::chrono::steady_clock::time_point lastFailure_;
  std::chrono::steady_clock::time_point startTime_;
  std::chrono::milliseconds currentInterval_;
  std::chrono::milliseconds nextBackOff_;
  std::atomic_bool halfOpen_;
  std::atomic_bool tripped_;
  std::atomic_bool broken_;
  std::mutex backOffLock_;
};

} // namespace circuitbreaker

} // namespace ycrt

#endif //YCRT_UTILS_CIRCUITBREAKER_CIRCUITBREAKER_H_
