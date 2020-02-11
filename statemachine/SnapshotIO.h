//
// Created by jason on 2020/2/11.
//

#ifndef YCRT_STATEMACHINE_SNAPSHOTIO_H_
#define YCRT_STATEMACHINE_SNAPSHOTIO_H_

#include <stdint.h>
#include <boost/filesystem.hpp>

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
  bool IsExported() { return Type == SnapshotRequestType::Exported; }
  bool IsStreaming() { return Type == SnapshotRequestType::Streaming; }
};

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
  CompressionType CompressionType;
};

class SnapshotWriter {
 public:
  StatusWith<uint64_t> Write(string_view content);
 private:
};

class SnapshotReader {
 public:
  StatusWith<uint64_t> Read(std::string &content);
 private:
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
    boost::filesystem::path path,// Filepath is the current full path of the file.
    string_view metadata);// Metadata is the metadata
};

} // namespace statemachine

} // namespace ycrt

#endif //YCRT_STATEMACHINE_SNAPSHOTIO_H_
