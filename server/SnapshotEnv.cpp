//
// Created by jason on 2020/2/5.
//

#include <spdlog/formatter.h>
#include "SnapshotEnv.h"

namespace ycrt
{

namespace server
{

using namespace std;
using namespace boost::filesystem;

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
    finalDir_(),
    filePath_()
{
}

} // namespace server

} // namespace ycrt
