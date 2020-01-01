//
// Created by jason on 2020/1/1.
//

#ifndef YCRT_TRANSPORT_NODES_H_
#define YCRT_TRANSPORT_NODES_H_

#include <functional>
#include <unordered_map>
#include <string>
#include <string_view>
#include "ycrt/Config.h"
#include "utils/Utils.h"
#include "pb/RaftMessage.h"

namespace ycrt
{

namespace transport
{

class Nodes {
 public:
  std::shared_ptr<Nodes> New(
    std::function<uint64_t(uint64_t)> &&partitionIDFunc);
  struct record {
    explicit record(const std::string &key);
    std::string key; // getConnectionKey(address+port, clusterID)  127.0.0.1:8800-5
    string_view address; // address + port
    uint32_t port;
  };
  void addRemoteAddress(
    uint64_t clusterID, uint64_t nodeID, const std::string &address);
  std::shared_ptr<record> resolve(uint64_t clusterID, uint64_t nodeID);
  std::vector<NodeInfo> reverseResolve(const std::string &address);
  void addNode(uint64_t clusterID, uint64_t nodeID, const std::string &address);
  void removeNode(uint64_t clusterID, uint64_t nodeID);
  void removeCluster(uint64_t clusterID);
  void removeAllPeers();
 private:
  Nodes();
  std::string getConnectionKey(const std::string &address, uint64_t clusterID);
  slogger log;
  std::function<uint64_t(uint64_t)> getPartitionID_;
  // store address specified by startCluster call
  std::mutex addrsMutex_;
  std::unordered_map<NodeInfo, std::shared_ptr<record>, NodeInfoHash> addrs_;
  // store remote nodes by exchanging messages
  std::mutex nodesMutex_;
  std::unordered_map<NodeInfo, std::shared_ptr<record>, NodeInfoHash> nodes_;
};
using NodesSPtr = std::shared_ptr<Nodes>;
using NodesRecordSPtr = std::shared_ptr<Nodes::record>;

class NodeHost;
class RaftMessageHandler {
 public:
  std::pair<uint64_t, uint64_t> handleMessageBatch(MessageBatchUPtr batch);
  void handleUnreachable(uint64_t clusterID, uint64_t nodeID);
  void handleSnapshotStatus(uint64_t clusterID, uint64_t nodeID, bool rejected);
  void handleSnapshot(uint64_t clusterID, uint64_t nodeID, uint64_t from);
 private:
  std::weak_ptr<NodeHost> nh_;
};
using RaftMessageHandlerSPtr = std::shared_ptr<RaftMessageHandler>;

} // namespace transport

} // namespace ycrt



#endif //YCRT_TRANSPORT_NODES_H_