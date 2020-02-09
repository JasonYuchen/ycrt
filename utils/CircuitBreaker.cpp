//
// Created by jason on 2020/1/9.
//

#include "CircuitBreaker.h"

namespace ycrt
{

namespace circuitbreaker
{

constexpr uint64_t Window::defaultTimeMS;
constexpr uint64_t Window::defaultBuckets;
constexpr uint64_t NaiveCircuitBreaker::defaultMultiplier;
constexpr uint64_t NaiveCircuitBreaker::defaultInitialIntervalMS;
constexpr uint64_t NaiveCircuitBreaker::defaultTimeoutIntervalMS;
constexpr std::chrono::milliseconds NaiveCircuitBreaker::Stop;

Window::Window(uint64_t windowTimeMS, uint64_t windowBuckets)
  : buckets_(windowBuckets),
    idx_(0),
    bucketTime_(windowTimeMS / windowBuckets),
    bucketLock_(),
    lastAccess_(std::chrono::steady_clock::now())
{
}

void Window::Fail()
{
  std::lock_guard<std::mutex> guard(bucketLock_);
  getLatestBucket()->fail();
}

void Window::Success()
{
  std::lock_guard<std::mutex> guard(bucketLock_);
  getLatestBucket()->success();
}

uint64_t Window::Failures()
{
  std::lock_guard<std::mutex> guard(bucketLock_);
  uint64_t count = 0;
  for (auto &item : buckets_) {
    count += item.failure_;
  }
  return count;
}

uint64_t Window::Successes()
{
  std::lock_guard<std::mutex> guard(bucketLock_);
  uint64_t count = 0;
  for (auto &item : buckets_) {
    count += item.success_;
  }
  return count;
}

void Window::Reset()
{
  std::lock_guard<std::mutex> guard(bucketLock_);
  for (auto &item : buckets_) {
    item.reset();
  }
}

Window::bucket *Window::getLatestBucket()
{
  auto elapsed = std::chrono::steady_clock::now() - lastAccess_;
  if (elapsed > bucketTime_) {
    for (size_t i = 0; i < buckets_.size(); ++i) {
      idx_++;
      buckets_[idx_ % buckets_.size()].reset();
      elapsed = elapsed - bucketTime_;
      if (elapsed < bucketTime_) {
        break;
      }
    }
    lastAccess_ = std::chrono::steady_clock::now();
  }
  return &buckets_[idx_ % buckets_.size()];
}

NaiveCircuitBreaker::NaiveCircuitBreaker()
  : shouldTrip_(),
    consecFailures_(0),
    counts_(Window::defaultTimeMS, Window::defaultBuckets),
    lastFailure_(),
    startTime_(),
    currentInterval_(defaultInitialIntervalMS),
    nextBackOff_(),
    halfOpen_(false),
    tripped_(false),
    broken_(false),
    backOffLock_()
{
  resetBackOff();
  setNextBackOff();
  // threshold trip function
  shouldTrip_ = [](NaiveCircuitBreaker *breaker){
    return breaker->Failures() == 1;
  };
}

bool NaiveCircuitBreaker::Ready()
{
  auto st = state();
  if (st == HALFOPEN) {
    halfOpen_ = false;
  }
  return st == CLOSED || st == HALFOPEN;
}

void NaiveCircuitBreaker::Success()
{
  {
    std::lock_guard<std::mutex> guard(backOffLock_);
    resetBackOff();
    setNextBackOff();
  }
  if (state() == HALFOPEN) {
    Reset();
  }
  consecFailures_ = 0;
  counts_.Success();
}

uint64_t NaiveCircuitBreaker::Successes()
{
  return counts_.Successes();
}

void NaiveCircuitBreaker::Fail()
{
  counts_.Fail();
  consecFailures_++;
  lastFailure_ = std::chrono::steady_clock::now();
  if (shouldTrip_ && shouldTrip_(this)) {
    Trip();
  }
}

uint64_t NaiveCircuitBreaker::Failures()
{
  return counts_.Failures();
}

uint64_t NaiveCircuitBreaker::ConsecFailures()
{
  return consecFailures_;
}

void NaiveCircuitBreaker::Reset()
{
  broken_ = false;
  tripped_ = false;
  halfOpen_ = false;
  consecFailures_ = 0;
  counts_.Reset();
}

void NaiveCircuitBreaker::Trip()
{
  tripped_ = true;
  lastFailure_ = std::chrono::steady_clock::now();
}

bool NaiveCircuitBreaker::Tripped()
{
  return tripped_;
}

NaiveCircuitBreaker::State NaiveCircuitBreaker::state()
{
  if (tripped_) {
    if (broken_) {
      return OPEN;
    }
    auto since = std::chrono::steady_clock::now() - lastFailure_;
    std::lock_guard<std::mutex> guard(backOffLock_);
    if (nextBackOff_ != Stop && since > nextBackOff_) {
      bool exp = false;
      if (halfOpen_.compare_exchange_strong(exp, true)) {
        setNextBackOff();
        return HALFOPEN;
      }
      return OPEN;
    }
    return OPEN;
  }
  return CLOSED;
}

void NaiveCircuitBreaker::resetBackOff()
{
  currentInterval_ = std::chrono::milliseconds(defaultInitialIntervalMS);
  startTime_ = std::chrono::steady_clock::now();
}

void NaiveCircuitBreaker::setNextBackOff()
{
  if (std::chrono::steady_clock::now() - startTime_
    > std::chrono::milliseconds(defaultTimeoutIntervalMS)) {
    nextBackOff_ = Stop;
  }
  currentInterval_ = currentInterval_ * defaultMultiplier;
  nextBackOff_ = currentInterval_;
}


} // namespace circuitbreaker

} // namespace ycrt