//
// Created by jason on 2020/1/1.
//

#ifndef YCRT_TRANSPORT_NODES_H_
#define YCRT_TRANSPORT_NODES_H_

#include <functional>
#include <unordered_map>
#include <string>
#include <string_view>
#include <boost/asio.hpp>
#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace transport
{

boost::asio::ip::tcp::endpoint getEndpoint(const std::string &addrPort);
boost::asio::ip::tcp::endpoint getEndpoint(string_view addrPort);

class Nodes {
 public:
  DISALLOW_COPY_AND_ASSIGN(Nodes);
  static std::unique_ptr<Nodes> New(
    std::function<uint64_t(uint64_t)> &&partitionIDFunc);
  struct Record {
    explicit Record(const std::string &key);
    std::string Key; // getConnectionKey(address+port, clusterID)  127.0.0.1:8800-5
    std::string Address; // address + port
    uint32_t Port;
  };
  void AddRemoteAddress(NodeInfo node, const std::string &address);
  std::shared_ptr<Record> Resolve(NodeInfo node);
  std::vector<NodeInfo> ReverseResolve(const std::string &address);
  void AddNode(NodeInfo node, const std::string &address);
  void RemoveNode(NodeInfo node);
  void RemoveCluster(uint64_t clusterID);
  void RemoveAllPeers();
 private:
  Nodes();
  std::string getConnectionKey(const std::string &address, uint64_t clusterID);
  slogger log;
  std::function<uint64_t(uint64_t)> getPartitionID_;
  // store address specified by startCluster call
  std::mutex addrsMutex_;
  std::unordered_map<NodeInfo, std::shared_ptr<Record>, NodeInfoHash> addrs_;
  // store remote nodes by exchanging messages
  std::mutex nodesMutex_;
  std::unordered_map<NodeInfo, std::shared_ptr<Record>, NodeInfoHash> nodes_;
};
using NodesSPtr = std::shared_ptr<Nodes>;
using NodesUPtr = std::unique_ptr<Nodes>;
using NodesRecordSPtr = std::shared_ptr<Nodes::Record>;

class RaftMessageHandler {
 public:
  std::pair<uint64_t, uint64_t> handleMessageBatch(pbMessageBatchUPtr batch)
  {
    Log.GetLogger("transport")->info("handleMessageBatch received: {}", batch->DebugString());
    return {0, 0};
  }
  void handleUnreachable(NodeInfo node)
  {
    Log.GetLogger("transport")->info("handleUnreachable received: {}", node);
  }
  void handleSnapshotStatus(NodeInfo node, bool rejected)
  {
    Log.GetLogger("transport")->info("handleSnapshotStatus received: {} {}", node, rejected);
  }
  void handleSnapshot(NodeInfo node, uint64_t from)
  {
    Log.GetLogger("transport")->info("handleSnapshot received: {} from {}", node, from);
  }
 private:
};

} // namespace transport

} // namespace ycrt



#endif //YCRT_TRANSPORT_NODES_H_
