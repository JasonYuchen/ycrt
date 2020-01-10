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
using namespace boost::asio;

ip::tcp::endpoint getEndpoint(const string &addrPort) // name:port
{
  auto pos = addrPort.find(':');
  return {ip::make_address(addrPort.substr(0, pos)),
          static_cast<unsigned short>(stoi(addrPort.substr(pos+1)))};
}

ip::tcp::endpoint getEndpoint(string_view addrPort) // name:port
{
  auto pos = addrPort.find(':');
  return {ip::make_address(addrPort.substr(0, pos)),
          static_cast<unsigned short>(stoi(addrPort.substr(pos+1).data()))};
}

NodesUPtr Nodes::New(function<uint64_t(uint64_t)> &&partitionIDFunc)
{
  NodesUPtr node(new Nodes());
  node->getPartitionID_ = std::move(partitionIDFunc);
  return node;
}

Nodes::Nodes()
  : log(Log.GetLogger("transport"))
{
}

Nodes::Record::Record(const std::string &key) // key = 127.0.0.1:8080-1
  : Key(key),
    Address(key.data(), key.find('-')),
    Port(std::stol(&key[key.find(':') + 1]))
{
}

void Nodes::AddRemoteAddress(
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
    nodes_[key] = make_shared<Record>(getConnectionKey(address, clusterID));
  } else if (node->Address != address) {
    log->error(
      "inconsistent address for {0:d}:{1:d}, received {2}, expected {3}",
      clusterID, nodeID, address, node->Address);
  }
}

NodesRecordSPtr Nodes::Resolve(uint64_t clusterID, uint64_t nodeID)
{
  assert(clusterID != 0);
  assert(nodeID != 0);
  NodeInfo key{clusterID, nodeID};
  NodesRecordSPtr addr;
  {
    lock_guard<mutex> guard(addrsMutex_);
    addr = addrs_[key];
  }
  if (addr == nullptr) {
    NodesRecordSPtr node;
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

vector<NodeInfo> Nodes::ReverseResolve(const string &address)
{
  assert(!address.empty());
  vector<NodeInfo> infos;
  lock_guard<mutex> guard(addrsMutex_);
  for (auto &addr : addrs_) {
    if (addr.second->Address == address) {
      infos.push_back(addr.first);
    }
  }
  return infos;
}

void Nodes::AddNode(
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
    addrs_[key] = make_shared<Record>(getConnectionKey(address, clusterID));
  }
}

void Nodes::RemoveNode(uint64_t clusterID, uint64_t nodeID)
{
  assert(clusterID != 0);
  assert(nodeID != 0);
  NodeInfo key{clusterID, nodeID};
  lock_guard<mutex> guard(addrsMutex_);
  addrs_.erase(key);
}

void Nodes::RemoveCluster(uint64_t clusterID) // set the sp to nullptr indicating the removal
{
  assert(clusterID != 0);
  lock_guard<mutex> guard(addrsMutex_);
  for (auto &addr : addrs_) {
    if (addr.first.ClusterID == clusterID) {
      addr.second.reset();
    }
  }
}

void Nodes::RemoveAllPeers()
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
