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
      auto last = list_.back();
      map_.erase(last->first);
      list_.pop_back();
    }
  }
  void Put(const K &key, V &&val) {
    list_.push_front({key, std::move(val)});
    remove(key);
    map_[key] = list_.begin();
    if (map_.size() > capacity_) {
      auto last = list_.back();
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
 private:
  void remove(const K &key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      onEvicted_(key, it->second->second);
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
