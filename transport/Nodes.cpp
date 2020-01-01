//
// Created by jason on 2020/1/1.
//

#include "Nodes.h"
#include <sstream>

namespace ycrt
{

namespace transport
{

using namespace std;

shared_ptr<Nodes> Nodes::New(function<uint64_t(uint64_t)> &&partitionIDFunc)
{
  shared_ptr<Nodes> node(new Nodes());
  node->getPartitionID_ = std::move(partitionIDFunc);
  return node;
}

Nodes::Nodes()
  : log(Log.get("transport"))
{
}

Nodes::record::record(const std::string &key)
  : key(key),
    address(key.data(), key.find(':')),
    port(std::stol(&key[key.find(':') + 1]))
{
}

void Nodes::addRemoteAddress(
  uint64_t clusterID,
  uint64_t nodeID,
  const string &address) // address = ip:port
{
  assert(clusterID != 0);
  assert(nodeID != 0);
  assert(!address.empty());
  NodeInfo key{clusterID, nodeID};
  lock_guard<mutex> guard(nodesMutex_);
  auto node = nodes_[key];
  if (node == nullptr) {
    nodes_[key] = make_shared<record>(getConnectionKey(address, clusterID));
  } else if (node->address != address) {
    log->error(
      "inconsistent address for {0:d}:{1:d}, received {2}, expected {3}",
      clusterID, nodeID, address, node->address);
  }
}

shared_ptr<Nodes::record> Nodes::resolve(uint64_t clusterID, uint64_t nodeID)
{
  assert(clusterID != 0);
  assert(nodeID != 0);
  NodeInfo key{clusterID, nodeID};
  shared_ptr<record> addr;
  {
    lock_guard<mutex> guard(addrsMutex_);
    addr = addrs_[key];
  }
  if (addr == nullptr) {
    shared_ptr<record> node;
    {
      lock_guard<mutex> guard(nodesMutex_);
      node = nodes_[key];
    }
    if (node == nullptr) {
      return nullptr; // errNotFound
    }
    {
      lock_guard<mutex> guard(addrsMutex_);
      addrs_[key] = node;
    }
    return node;
  }
  return addr;
}

vector<NodeInfo> Nodes::reverseResolve(const string &address)
{
  assert(!address.empty());
  vector<NodeInfo> infos;
  lock_guard<mutex> guard(addrsMutex_);
  for (auto &addr : addrs_) {
    if (addr.second->address == address) {
      infos.push_back(addr.first);
    }
  }
  return infos;
}

void Nodes::addNode(
  uint64_t clusterID,
  uint64_t nodeID,
  const string &address) // url = ip:port
{
  assert(clusterID != 0);
  assert(nodeID != 0);
  assert(!address.empty());
  NodeInfo key{clusterID, nodeID};
  lock_guard<mutex> guard(addrsMutex_);
  if (addrs_.find(key) == addrs_.end()) {
    addrs_[key] = make_shared<record>(getConnectionKey(address, clusterID));
  }
}

void Nodes::removeNode(uint64_t clusterID, uint64_t nodeID)
{
  assert(clusterID != 0);
  assert(nodeID != 0);
  NodeInfo key{clusterID, nodeID};
  lock_guard<mutex> guard(addrsMutex_);
  addrs_.erase(key);
}

void Nodes::removeCluster(uint64_t clusterID) // set the sp to nullptr indicating the removal
{
  assert(clusterID != 0);
  lock_guard<mutex> guard(addrsMutex_);
  for (auto &addr : addrs_) {
    if (addr.first.ClusterID == clusterID) {
      addr.second.reset();
    }
  }
}

void Nodes::removeAllPeers()
{
  lock_guard<mutex> guard(addrsMutex_);
  addrs_.clear();
}

string Nodes::getConnectionKey(const string &address, uint64_t clusterID)
{
  stringstream key;
  key << address << '-' << getPartitionID_(clusterID);
  return key.str();
}

} // namespace transport

} // namespace ycrt
