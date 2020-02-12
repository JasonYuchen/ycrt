//
// Created by jason on 2020/2/10.
//

#ifndef YCRT_YCRT_STATEMACHINE_H_
#define YCRT_YCRT_STATEMACHINE_H_

#include <stdint.h>
#include <statemachine/SnapshotIO.h>
#include "utils/Utils.h"

namespace ycrt
{

using statemachine::SnapshotReader;
using statemachine::SnapshotWriter;
using statemachine::SnapshotFileSet;


// Value + size + Data, size = len(Data)
struct Result {
  uint64_t Value;
  any Data;
  // FIXME:
  size_t AppendToString(std::string &buf) const {
    size_t size = any_cast<std::string&>(Data).size();
    buf.append(reinterpret_cast<const char *>(&Value), sizeof(Value));
    buf.append(reinterpret_cast<const char *>(&size), sizeof(size));
    buf.append(any_cast<std::string&>(Data));
    return sizeof(Value) + sizeof(size) + size;
  }
  // FIXME:
  size_t FromString(string_view buf) {
    size_t size;
    if (buf.size() < sizeof(Value) + sizeof(size)) {
      throw Error(ErrorCode::ShortRead);
    }
    ::memcpy(&Value, buf.data(), sizeof(Value));
    ::memcpy(&size, buf.data()+sizeof(Value), sizeof(size));
    if (buf.size() - sizeof(Value) - sizeof(size) < size) {
      throw Error(ErrorCode::ShortRead);
    }
    Data = std::string(buf.data()+sizeof(Value)+sizeof(size), size);
    return sizeof(Value) + sizeof(size) + size;
  }
};
using ResultSPtr = std::shared_ptr<Result>;

struct Entry {
  uint64_t Index;
  string_view Cmd;
  struct Result Result;
};

struct SnapshotFile {
  uint64_t FileID;
  boost::filesystem::path FilePath;
  std::string Metadata;
};

class RegularStateMachine {
 public:
  virtual Status Update(Entry &entry) = 0;
  virtual StatusWith<any> Lookup(any query) = 0;
  virtual Status SaveSnapshot(
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) = 0;
  virtual Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) = 0;
  virtual Status Close() = 0;
};

class ConcurrentStateMachine {
 public:
  virtual Status Update(std::vector<Entry> &entries) = 0;
  virtual StatusWith<any> Lookup(any query) = 0;
  virtual StatusWith<any> PrepareSnapshot() = 0;
  virtual Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) = 0;
  virtual Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) = 0;
  virtual Status Close() = 0;
};

class OnDiskStateMachine {
 public:
  virtual StatusWith<uint64_t> Open(std::atomic_bool &stopped) = 0;
  virtual Status Update(std::vector<Entry> &entries) = 0;
  virtual StatusWith<any> Lookup(any query) = 0;
  virtual Status Sync() = 0;
  virtual StatusWith<any> PrepareSnapshot() = 0;
  virtual Status SaveSnapshot(
    any context,
    SnapshotWriter &writer,
    SnapshotFileSet &files,
    std::atomic_bool &stopped) = 0;
  virtual Status RecoverFromSnapshot(
    SnapshotReader &reader,
    const std::vector<SnapshotFile> &files,
    std::atomic_bool &stopped) = 0;
  virtual Status Close() = 0;
};


} // namespace ycrt

#endif //YCRT_YCRT_STATEMACHINE_H_
