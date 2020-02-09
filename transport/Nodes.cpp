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

// AddRemoteAddress remembers the specified address obtained from the source
// of the incoming message.
void Nodes::AddRemoteAddress(NodeInfo node, const string &address) // address = ip:port
{
  assert(node.Valid());
  assert(!address.empty());
  lock_guard<mutex> guard(nodesMutex_);
  auto nodeRecord = nodes_[node];
  if (nodeRecord == nullptr) {
    nodes_[node] = make_shared<Record>(
      getConnectionKey(address, node.ClusterID));
  } else if (nodeRecord->Address != address) {
    log->error("inconsistent address for {}, received {}, expected {}",
      node, address, nodeRecord->Address);
  }
}

// Resolve looks up the Addr of the specified node.
NodesRecordSPtr Nodes::Resolve(NodeInfo node)
{
  assert(node.Valid());
  NodesRecordSPtr addr;
  {
    lock_guard<mutex> guard(addrsMutex_);
    auto it = addrs_.find(node);
    if (it != addrs_.end()) {
      addr = it->second;
    }
  }
  if (addr == nullptr) {
    NodesRecordSPtr nodeRecord;
    {
      lock_guard<mutex> guard(nodesMutex_);
      auto it = nodes_.find(node);
      if (it != nodes_.end()) {
        nodeRecord = it->second;
      }
    }
    if (nodeRecord == nullptr) {
      return nullptr; // errNotFound
    }
    {
      lock_guard<mutex> guard(addrsMutex_);
      addrs_[node] = nodeRecord;
    }
    return nodeRecord;
  }
  return addr;
}

// ReverseResolve does the reverse lookup for the specified address. A list
// of node NodeInfos are returned for nodes that match the specified address
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

// AddNode add a new node.
void Nodes::AddNode(NodeInfo node, const string &address) // url = ip:port
{
  assert(node.Valid());
  assert(!address.empty());
  lock_guard<mutex> guard(addrsMutex_);
  if (addrs_.find(node) == addrs_.end()) {
    addrs_[node] = make_shared<Record>(
      getConnectionKey(address, node.ClusterID));
  }
}

void Nodes::RemoveNode(NodeInfo node)
{
  assert(node.Valid());
  lock_guard<mutex> guard(addrsMutex_);
  addrs_.erase(node);
}

// RemoveCluster removes all nodes info associated with the specified cluster
void Nodes::RemoveCluster(uint64_t clusterID) // nullptr indicating the removal
{
  assert(clusterID != 0);
  lock_guard<mutex> guard(addrsMutex_);
  for (auto &addr : addrs_) {
    if (addr.first.ClusterID == clusterID) {
      addr.second.reset();
    }
  }
}

// RemoveAllPeers removes all remotes.
void Nodes::RemoveAllPeers()
{
  lock_guard<mutex> guard(addrsMutex_);
  addrs_.clear();
}

string Nodes::getConnectionKey(const string &address, uint64_t clusterID)
{
  return fmt::format("{}-{}", address, getPartitionID_(clusterID));
}

} // namespace transport

} // namespace ycrt
