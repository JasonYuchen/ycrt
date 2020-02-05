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

void SnapshotEnv::createDir(const path &dir)
{
  // FIXME: dir must be the direct child of rootDir
  //  if (dir.parent_path() != rootDir_) {
  //    throw Error();
  //  }
  error_code ec;
  create_directory(dir, ec);
  if (ec) {
    throw Error(ErrorCode::SnapshotEnvError,
      "failed to create directory={0} with error_code={1}", dir, ec.message());
  }
  // fsync

}

void SnapshotEnv::removeDir(const path &dir)
{
  // dir must be the direct child of rootDir
}

} // namespace server

} // namespace ycrt
