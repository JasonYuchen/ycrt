//
// Created by jason on 2020/1/6.
//

#ifndef YCRT_PB_PBRAFT_H_
#define YCRT_PB_PBRAFT_H_

#include "settings/Hard.h"
#include <stdint.h>
#include <unordered_map>
#include <memory>
#include <string>
#include <string.h>
#include <vector>
#include <spdlog/fmt/fmt.h>
#include "utils/Types.h"

// TODO:
//  https://github.com/fraillt/cpp_serializers_benchmark

namespace ycrt
{

namespace pb
{

enum MessageType : uint8_t {
  NoOP = 0,
  LocalTick,
  Election,
  LeaderHeartbeat,
  ConfigChangeEvent,
  Propose,
  SnapshotStatus,
  SnapshotReceived,
  InstallSnapshot,
  Unreachable,
  CheckQuorum,
  BatchedReadIndex,
  Replicate,
  ReplicateResp,
  RequestPreVote,
  RequestPreVoteResp,
  RequestVote,
  RequestVoteResp,
  Heartbeat,
  HeartbeatResp,
  ReadIndex,
  ReadIndexResp,
  Quiesce,
  LeaderTransfer,
  TimeoutNow,
  NumOfMessageType,
};

enum EntryType : uint8_t {
  ApplicationEntry = 0,
  ConfigChangeEntry,
  EncodedEntry,
  MetadataEntry,
  NumOfEntryType,
};

enum ConfigChangeType : uint8_t {
  AddNode = 0,
  RemoveNode,
  AddObserver,
  AddWitness,
  NumOfConfigChangeType,
};

enum StateMachineType : uint8_t {
  RegularStateMachine = 0,
  ConcurrentStateMachine,
  OnDiskStateMachine,
  NumOfStateMachineType,
};

enum CompressionType : uint8_t {
  NoCompression = 0,
  Snappy,
  NumOfCompressionType,
};

enum ChecksumType : uint8_t {
  CRC32IEEE = 0,
  HIGHWAY,
  NumOfChecksumType,
};

constexpr uint64_t RequestHeaderSize = 16;
constexpr uint64_t PayloadBufSize = settings::SnapshotChunkSize + 1024 * 128;
constexpr uint32_t RequestType = 100;
constexpr uint32_t SnapshotChunkType = 200;

struct RequestHeader {
  uint32_t Method;
  uint32_t CRC32;
  uint64_t Size;
  // FIXME
  void Encode(char *buf, size_t len) {
    ::memcpy(&buf[0], &Method, 4);
    ::memcpy(&buf[4], &CRC32, 4);
    ::memcpy(&buf[8], &Size, 8);
  }
  // FIXME
  static RequestHeader Decode(const char *buf, size_t len) {
    RequestHeader header{};
    ::memcpy(&header.Method, &buf[0], 4);
    ::memcpy(&header.CRC32, &buf[4], 4);
    ::memcpy(&header.Size, &buf[8], 8);
    return header;
  }
};

static_assert(RequestHeaderSize == sizeof(RequestHeader),
  "RequestHeaderSize != 16");

class Buffer {
 public:
  void Append(const char *data, uint64_t len);
  void Append(const std::string &data);
  void Append(uint64_t data);
  void Append(uint32_t data);
  void Append(uint16_t data);
  void Append(uint8_t data);
 private:
  char *data;
  uint64_t length;
};

class Bootstrap {
 public:
  Bootstrap() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(Bootstrap);
 private:
  std::array<bool, 3> field = {};
  std::unordered_map<uint64_t, std::string> addresses;  // 1
  bool join = false;                                    // 2
  StateMachineType type = RegularStateMachine;          // 3
};
using BootstrapSPtr = std::shared_ptr<Bootstrap>;

class RaftDataStatus {
 public:
  RaftDataStatus() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(RaftDataStatus);
 private:
  std::array<bool, 10> field = {};
  std::string address;                                  // 1
  uint32_t binVersion = 0;                              // 2
  uint64_t hardHash = 0;                                // 3
  std::string logdbType;                                // 4
  std::string hostName;                                 // 5
  uint64_t deploymentID = 0;                            // 6
  uint64_t stepWorkerCount = 0;                         // 7
  uint64_t logdbShardCount = 0;                         // 8
  uint64_t maxSessionCount = 0;                         // 9
  uint64_t entryBatchSize = 0;                          //10
};
using RaftDataStatusSPtr = std::shared_ptr<RaftDataStatus>;

class State {
 public:
  State() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(State);
 private:
  std::array<bool, 3> field = {};
  uint64_t term = 0;                                    // 1
  uint64_t vote = 0;                                    // 2
  uint64_t commit = 0;                                  // 3
};
using StateSPtr = std::shared_ptr<State>;

class Entry {
 public:
  Entry() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(Entry);
 private:
  std::array<bool, 8> field = {};
  EntryType type = ApplicationEntry;                    // 1
  uint64_t term = 0;                                    // 2
  uint64_t index = 0;                                   // 3
  uint64_t key = 0;                                     // 4
  uint64_t clientID = 0;                                // 5
  uint64_t seriesID = 0;                                // 6
  uint64_t respondedTo = 0;                             // 7
  std::string cmd;                                      // 8
};
using EntrySPtr = std::shared_ptr<Entry>;

class Membership {
 public:
  Membership() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(Membership);
 private:
  std::array<bool, 5> field = {};
  uint64_t configChangeID = 0;                          // 1
  std::unordered_map<uint64_t, std::string> addresses;  // 2
  std::unordered_map<uint64_t, bool> removed;           // 3
  std::unordered_map<uint64_t, std::string> observers;  // 4
  std::unordered_map<uint64_t, std::string> witnesses;  // 5
};
using MembershipSPtr = std::shared_ptr<Membership>;

class SnapshotFile {
 public:
  SnapshotFile() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(SnapshotFile);
 private:
  std::array<bool, 4> field = {};
  std::string filePath;                                 // 1
  uint64_t fileSize = 0;                                // 2
  uint64_t fileID = 0;                                  // 3
  std::string metaData;                                 // 4
};
using SnapshotFileSPtr = std::shared_ptr<SnapshotFile>;

class SnapshotHeader {
 public:
  SnapshotHeader() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(SnapshotHeader);
 private:
  std::array<bool, 8> field = {};
  uint64_t sessionSize = 0;                             // 1
  uint64_t dataStoreSize = 0;                           // 2
  uint64_t unreliableTime = 0;                          // 3
  std::string gitVersion;                               // 4
  std::string headerChecksum;                           // 5
  ChecksumType checksumType = CRC32IEEE;                // 6
  uint64_t version = 0;                                 // 7
  CompressionType compressionType = NoCompression;      // 8
};
using SnapshotHeaderSPtr = std::shared_ptr<SnapshotHeader>;

class SnapshotChunk {
 public:
  SnapshotChunk() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(SnapshotChunk);
 private:
  std::array<bool, 19> field = {};
  uint64_t clusterID = 0;                               // 1
  uint64_t nodeID = 0;                                  // 2
  uint64_t from = 0;                                    // 3
  uint64_t chunkID = 0;                                 // 4
  uint64_t chunkSize = 0;                               // 5
  uint64_t chunkCount = 0;                              // 6
  std::string data;                                     // 7
  uint64_t index = 0;                                   // 8
  uint64_t term = 0;                                    // 9
  MembershipSPtr membership;                            //10
  std::string filePath;                                 //11
  uint64_t fileSize = 0;                                //12
  uint64_t deploymentID = 0;                            //13
  uint64_t fileChunkID = 0;                             //14
  uint64_t fileChunkCount = 0;                          //15
  SnapshotFileSPtr snapshotFile;                        //16
  uint64_t binVersion = 0;                              //17
  uint64_t onDiskIndex = 0;                             //18
  bool witness = false;                                 //19
};

class Snapshot {
 public:
  Snapshot() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(Snapshot);
 private:
  std::array<bool, 13> field = {};
  std::string filePath;                                 // 1
  uint64_t fileSize = 0;                                // 2
  uint64_t index = 0;                                   // 3
  uint64_t term = 0;                                    // 4
  MembershipSPtr membership;                            // 5
  std::vector<SnapshotFile> snapshotFiles;              // 6
  std::string checksum;                                 // 7
  bool dummy = false;                                   // 8
  uint64_t clusterID = 0;                               // 9
  StateMachineType type = RegularStateMachine;          //10
  bool imported = false;                                //11
  uint64_t onDiskIndex = 0;                             //12
  bool witness = false;                                 //13
};
using SnapshotSPtr = std::shared_ptr<Snapshot>;

class Message {
 public:
  Message() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(Message);
 private:
  std::array<bool, 13> field = {};
  MessageType type = NoOP;                              // 1
  uint64_t to = 0;                                      // 2
  uint64_t from = 0;                                    // 3
  uint64_t clusterID = 0;                               // 4
  uint64_t term = 0;                                    // 5
  uint64_t logTerm = 0;                                 // 6
  uint64_t logIndex = 0;                                // 7
  uint64_t commit = 0;                                  // 8
  uint64_t reject = 0;                                  // 9
  uint64_t hint = 0;                                    //10
  uint64_t hintHigh = 0;                                //11
  std::vector<Entry> entries;                           //12
  SnapshotSPtr snapshot;                                //13
};
using MessageSPtr = std::shared_ptr<Message>;

class MessageBatch {
 public:
  MessageBatch() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(MessageBatch);
 private:
  std::array<bool, 4> field = {};
  uint64_t deploymentID = 0;                            // 1
  std::string sourceAddress;                            // 2
  uint64_t binVersion = 0;                              // 3
  std::vector<Message> requests;                        // 4
};
using MessageBatchSPtr = std::shared_ptr<MessageBatch>;

class ConfigChange {
 public:
  ConfigChange() = default;
  DEFAULT_COPY_MOVE_AND_ASSIGN(ConfigChange);
 private:
  std::array<bool, 5> field = {};
  ConfigChangeType type = AddNode;                      // 1
  uint64_t nodeID = 0;                                  // 2
  std::string address;                                  // 3
  bool initialize = false;                              // 4
};
using ConfigChangeSPtr = std::shared_ptr<ConfigChange>;

} // namepsace pb

} // namespace ycrt


#endif //YCRT_PB_PBRAFT_H_
