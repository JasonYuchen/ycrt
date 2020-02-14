//
// Created by jason on 2020/2/13.
//

#ifndef YCRT_UTILS_DATASTRUCTURE_H_
#define YCRT_UTILS_DATASTRUCTURE_H_

#include <stdint.h>
#include <unordered_map>
#include <list>
#include <functional>

namespace ycrt
{

class Buffer {
 public:
  Buffer()
    : cur_(0), buf_() {}
  const char *Data() const
  {
    return buf_.data();
  }
  char *Data()
  {
    return buf_.data();
  }
  uint64_t Written() const
  {
    return cur_;
  }
  Buffer &Skip(uint64_t len)
  {
    ensure(len);
    cur_ += len;
    return *this;
  }
  Buffer &Append(string_view data)
  {
    return Append(data.data(), data.size());
  }
  Buffer &Append(const char *data, uint64_t len)
  {
    ensure(len);
    ::memcpy(buf_.data() + cur_, data, len);
    cur_ += len;
    return *this;
  }
  Buffer &Append(std::string &&data)
  {
    return Append(data);
  }
  Buffer &Append(const std::string &data)
  {
    ensure(data.size());
    ::memcpy(buf_.data() + cur_, data.data(), data.size());
    cur_ += data.size();
    return *this;
  }
  template<typename T>
  Buffer &Append(T data)
  {
    static_assert(std::is_arithmetic<T>::value, "append arithmetic");
    ensure(sizeof(data));
    ::memcpy(buf_.data() + cur_, &data, sizeof(data));
    cur_ += sizeof(data);
    return *this;
  }
 private:
  void ensure(uint64_t len)
  {
    if (cur_ + len > buf_.size()) {
      buf_.resize(2 * buf_.size());
    }
  }
  uint64_t cur_;
  std::vector<char> buf_;
};

template<typename K, typename V>
class LRUCache {
 public:
  explicit LRUCache(
    size_t capacity,
    std::function<void(const K &,V &)> &&onEvicted = [](const K &, V &){})
    : capacity_(capacity), onEvicted_(std::move(onEvicted))
  {}
  size_t Size() const { return map_.size(); }
  size_t Capacity() const { return capacity_; }
  void Put(const K &key, const V &val) {
    list_.push_front({key, val});
    remove(key);
    map_[key] = list_.begin();
    if (map_.size() > capacity_) {
      auto last = list_.end()--;
      onEvicted_(last->first, last->second);
      map_.erase(last->first);
      list_.pop_back();
    }
  }
  void Put(const K &key, V &&val) {
    list_.push_front({key, std::move(val)});
    remove(key);
    map_[key] = list_.begin();
    if (map_.size() > capacity_) {
      auto last = list_.end()--;
      onEvicted_(last->first, last->second);
      map_.erase(last->first);
      list_.pop_back();
    }
  }
  void Del(const K &key) {
    remove(key);
  }
  bool Get(const K &key, V &val) const {
    auto it = map_.find(key);
    if (it == map_.end()) {
      return false;
    } else {
      val = it->second->second;
      return true;
    }
  }
  bool Has(const K &key) const {
    return map_.find(key) != map_.end();
  }
  void OrderedDo(std::function<void(const K &,V &)> &&func) {
    for (auto &item : list_) {
      func(item->first, item->second);
    }
  }
 private:
  void remove(const K &key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      list_.erase(it->second);
      map_.erase(key);
    }
  }
  const size_t capacity_;
  std::list<std::pair<K, V>> list_;
  using entry = typename std::list<std::pair<K, V>>::iterator;
  std::unordered_map<K, entry> map_;
  std::function<void(const K &,V &)> onEvicted_;
};

} // namespace ycrt

#endif //YCRT_UTILS_DATASTRUCTURE_H_
