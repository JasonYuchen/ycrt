//
// Created by jason on 2020/2/11.
//

#ifndef YCRT_STATEMACHINE_SNAPSHOTIO_H_
#define YCRT_STATEMACHINE_SNAPSHOTIO_H_

#include <stdint.h>
#include <boost/filesystem.hpp>
#include <logdb/LogDB.h>

#include "server/SnapshotEnv.h"
#include "utils/Utils.h"
#include "pb/RaftMessage.h"
#include "ycrt/Config.h"

namespace ycrt
{

namespace statemachine
{

enum class SnapshotRequestType : uint8_t {
  Periodic,
  UserRequested,
  Exported,
  Streaming,
};

struct SnapshotRequest {
  SnapshotRequestType Type;
  uint64_t Key;
  boost::filesystem::path Path;
  bool OverrideCompaction;
  uint64_t CompactionOverhead;
  bool IsExported() const { return Type == SnapshotRequestType::Exported; }
  bool IsStreaming() const { return Type == SnapshotRequestType::Streaming; }
};
using SnapshotRequestSPtr = std::shared_ptr<SnapshotRequest>;

struct SnapshotMeta {
  uint64_t From;
  uint64_t Index;
  uint64_t Term;
  uint64_t OnDiskIndex;
  SnapshotRequest Request;
  pbMembershipSPtr Membership;
  pbStateMachineType StateMachineType;
  std::string Session;
  any Context;
  enum CompressionType CompressionType;
};

// TODO: currently a naive implementation of SnapshotWriter
//  consider compressor, buffering, etc
class SnapshotWriter {
 public:
  SnapshotWriter(boost::filesystem::path path, CompressionType type);
  uint64_t Write(string_view content);
  uint64_t GetPayloadSize() const;
  // TODO: use crc32 for checksum
  uint64_t GetPayloadChecksum() const;
  ~SnapshotWriter();
 private:
  int fd_;
  boost::filesystem::path fp_;
  uint64_t writtenBytes_;
};

// TODO: currently a naive implementation of SnapshotReader
//  consider compressor, buffering, etc
class SnapshotReader {
 public:
  SnapshotReader(boost::filesystem::path path, CompressionType type);
  uint64_t Read(std::string &content);
  ~SnapshotReader();
 private:
  int fd_;
  boost::filesystem::path fp_;
};

class SnapshotFileSet {
 public:
  // AddFile adds an external file to the snapshot being currently generated.
  // The file must has been finalized meaning its content will not change in
  // the future. It is your application's responsibility to make sure that the
  // file being added can be accessible from the current process and it is
  // possible to create a hard link to it from the NodeHostDir directory
  // specified in NodeHost's NodeHostConfig. The file to be added is identified
  // by the specified fileID. The metadata byte slice is the metadata of the
  // file being added, it can be the checksum of the file, file type, file name,
  // other file hierarchy information, or a serialized combination of such
  // metadata.
  void AddFile(
    uint64_t fileID,// FileID is the ID of the file
    const boost::filesystem::path &path,// Filepath is the current full path of the file.
    std::string metadata);// Metadata is the metadata
  size_t Size() const;
  pbSnapshotFileSPtr GetFile(uint64_t index);
  // After PrepareFiles, the underlying files_ will be empty
  std::vector<pbSnapshotFileSPtr> PrepareFiles(
    const server::SnapshotEnv &env);
 private:
  std::vector<pbSnapshotFileSPtr> files_;
  std::unordered_set<uint64_t> ids_;
};


} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_SNAPSHOTIO_H_
