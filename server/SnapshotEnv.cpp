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

static path getTmpDir(
  const path &rootDir,
  const char *suffix,
  uint64_t index,
  uint64_t from)
{
  return rootDir / fmt::format("snapshot-{0:016X}-{1}.{2}", index, from, suffix);
}

static path getFinalDir(const path &rootDir, uint64_t index)
{
  return rootDir / fmt::format("snapshot-{0:016X}", index);
}

static path getFileName(uint64_t index)
{
  return fmt::format("snapshot-{0:016X}.{1}", index, fileSuffix);
}

static path getShrunkFileName(uint64_t index)
{
  return fmt::format("snapshot-{0:016X}.{1}", index, shrunkSuffix);
}

SnapshotEnv::SnapshotEnv(
  SnapshotLocator &locator,
  uint64_t clusterID,
  uint64_t nodeID,
  uint64_t index,
  uint64_t from,
  Mode mode)
  : index_(index),
    rootDir_(locator(clusterID, nodeID)),
    tmpDir_(getTmpDir(rootDir_, getSuffix(mode), index, from)),
    finalDir_(getFinalDir(rootDir_, index)),
    filePath_(finalDir_ / getFileName(index))
{
}

Status SnapshotEnv::FinalizeSnapshot()
{
  lock_guard<mutex> guard(finalizeLock_);
  // FIXME: flag file?
  if (exists(finalDir_)) {
    return ErrorCode::SnapshotOutOfDate;
  }
  error_code ec;
  rename(tmpDir_, finalDir_, ec);
  if (ec) {
    return ErrorCode::SnapshotEnvError;
  }
  return SyncDir(rootDir_);
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

Status SnapshotEnv::createDir(const path &dir, bool must)
{
  // FIXME: dir must be the direct child of rootDir
  //  if (dir.parent_path() != rootDir_) {
  //    throw Error();
  //  }
  error_code ec;
  bool done = create_directory(dir, ec);
  if (!done || ec) {
    if (must) {
      throw Error(ErrorCode::SnapshotEnvError,
        "failed to create directory={0} with error={1}",
        dir.c_str(), ec.message());
    }
    return ErrorCode::SnapshotEnvError;
  }
  return SyncDir(dir);
}

Status SnapshotEnv::removeDir(const path &dir, bool must)
{
  // FIXME: dir must be the direct child of rootDir
  //  if (dir.parent_path() != rootDir_) {
  //    throw Error();
  //  }
  error_code ec;
  bool done = remove(dir, ec);
  if (!done || ec) {
    if (must) {
      throw Error(ErrorCode::SnapshotEnvError,
        "failed to remove directory={0} with error={1}",
        dir.c_str(), ec.message());
    }
    return ErrorCode::SnapshotEnvError;
  }
  return SyncDir(dir);
}

std::mutex SnapshotEnv::finalizeLock_;

} // namespace server

} // namespace ycrt
