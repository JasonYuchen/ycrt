//
// Created by jason on 2020/1/1.
//

#ifndef YCRT_TRANSPORT_NODES_H_
#define YCRT_TRANSPORT_NODES_H_

#include <functional>
#include <unordered_map>
#include "ycrt/Config.h"
#include "utils/Utils.h"

namespace ycrt
{

namespace transport
{

class Nodes {
 public:
  struct addr {
    std::string network;
    std::string address;
    uint64_t port;
  };
  struct record {
    std::string address;
    std::string key;
  };
  void addRemoteAddress(uint64_t clusterID, uint64_t nodeID, std::string &address)
  {
    assert(clusterID != 0);
    assert(nodeID != 0);
    assert(!address.empty());
    auto key = NodeInfo{clusterID, nodeID};
    std::lock_guard<std::mutex> guard(nodesMutex_);
    auto node = nodes_.find(key);
    if (node == nodes_.end()) {
      nodes_[key] = address;
    } else if (node->second != address) {
      log->error(
        "inconsistent address for {0:d}:{1:d}, received {2}, expected {3}",
        clusterID, nodeID, address, node->second);
    }
  }
  record resolve(uint64_t clusterID, uint64_t nodeID)
  {
    assert(clusterID != 0);
    assert(nodeID != 0);
    auto key = NodeInfo{clusterID, nodeID};
    addrsMutex_.lock();
    auto addr = addrs_.find(key);
    addrsMutex_.unlock();
    if (addr == addrs_.end()) {
      // FIXME
    }

    return addr->second;
  }
 private:
  slogger log;
  std::function<uint64_t(uint64_t)> getPartitionID_;
  std::mutex addrsMutex_;
  std::unordered_map<NodeInfo, record, NodeInfoHash> addrs_;
  std::mutex nodesMutex_;
  std::unordered_map<NodeInfo, std::string, NodeInfoHash> nodes_;
};

} // namespace transport

} // namespace ycrt



#endif //YCRT_TRANSPORT_NODES_H_
