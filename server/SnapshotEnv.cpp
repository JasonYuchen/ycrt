//
// Created by jason on 2020/2/5.
//

#include <spdlog/formatter.h>
#include "utils/Error.h"
#include "SnapshotEnv.h"

namespace ycrt
{

namespace server
{

using namespace std;
using namespace boost::filesystem;
using boost::system::error_code;

static const char *snapshotFlagFile = "snapshot.message";
static const char *snapshotMetadataFlagFile = "snapshot.metadata";
static const char *fileSuffix = "snap";
static const char *genTmpDirSuffix = "generating";
static const char *recvTmpDirSuffix = "receiving";
static const char *shrunkSuffix = "shrunk";

static const char *getSuffix(SnapshotEnv::Mode mode)
{
  if (mode == SnapshotEnv::Snapshotting) {
    return genTmpDirSuffix;
  } else {
    return recvTmpDirSuffix;
  }
}

// use 020X (the max number of type uint64_t)
static path getTmpDir(
  const path &rootDir,
  const char *suffix,
  uint64_t index,
  uint64_t from)
{
  return rootDir / fmt::format("snapshot-{:020X}-{}.{}", index, from, suffix);
}

static path getFinalDir(const path &rootDir, uint64_t index)
{
  return rootDir / fmt::format("snapshot-{:020X}", index);
}

static path getFileName(uint64_t index)
{
  return fmt::format("snapshot-{:020X}.{}", index, fileSuffix);
}

static path getShrunkFileName(uint64_t index)
{
  return fmt::format("snapshot-{:020X}.{}", index, shrunkSuffix);
}

SnapshotEnv::SnapshotEnv(
  const string &root,
  uint64_t index,
  uint64_t from,
  Mode mode)
  : index_(index),
    rootDir_(root),
    tmpDir_(getTmpDir(rootDir_, getSuffix(mode), index, from)),
    finalDir_(getFinalDir(rootDir_, index)),
    filePath_(finalDir_ / getFileName(index))
{
}

path SnapshotEnv::GetFilePath() const
{
  return finalDir_ / getFileName(index_);
}

path SnapshotEnv::GetShrunkFilePath() const
{
  return finalDir_ / getShrunkFileName(index_);
}

path SnapshotEnv::GetTempFilePath() const
{
  return tmpDir_ / getFileName(index_);
}

void SnapshotEnv::SaveSnapshotMetadata(string_view metadata)
{
  CreateFlagFile(tmpDir_ / snapshotMetadataFlagFile, metadata);
}

bool SnapshotEnv::HasFlagFile()
{
  return exists(finalDir_ / snapshotFlagFile);
}

void SnapshotEnv::RemoveFlagFile()
{
  ycrt::RemoveFlagFile(finalDir_ / snapshotFlagFile);
}

Status SnapshotEnv::FinalizeSnapshot(string_view snapshotMsgRaw)
{
  lock_guard<mutex> guard(finalizeLock_);
  CreateFlagFile(tmpDir_ / snapshotFlagFile, snapshotMsgRaw);
  bool finalExisting = exists(finalDir_);
  if (finalExisting) {
    return ErrorCode::SnapshotOutOfDate;
  }
  rename(tmpDir_, finalDir_);
  SyncDir(rootDir_);
  return ErrorCode::OK;
}

void SnapshotEnv::createDir(const path &dir)
{
  // FIXME: dir must be the direct child of rootDir
  //  if (dir.parent_path() != rootDir_) {
  //    throw Error();
  //  }
  create_directory(dir);
  SyncDir(rootDir_); //dir must be the direct child of rootDir
}

void SnapshotEnv::removeDir(const path &dir)
{
  // FIXME: dir must be the direct child of rootDir
  //  if (dir.parent_path() != rootDir_) {
  //    throw Error();
  //  }
  remove_all(dir);
  SyncDir(rootDir_); //dir must be the direct child of rootDir
}

std::mutex SnapshotEnv::finalizeLock_;

} // namespace server

} // namespace ycrt
