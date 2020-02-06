//
// Created by jason on 2020/2/5.
//

#ifndef YCRT_SERVER_SNAPSHOTENV_H_
#define YCRT_SERVER_SNAPSHOTENV_H_

#include <stdint.h>
#include <string>
#include <functional>
#include "FileUtils.h"

namespace ycrt
{

namespace server
{

// dir SnapshotLocator(clusterID, nodeID)
using SnapshotLocator = std::function<std::string(uint64_t, uint64_t)>;

class SnapshotEnv {
 public:
  enum Mode { Snapshotting, Receiving };
  SnapshotEnv(
    SnapshotLocator &locator,
    uint64_t clusterID,
    uint64_t nodeID,
    uint64_t index,
    uint64_t from,
    Mode mode);
  // FIXME: FinalizeSnapshot argument proto.Message ?
  Status FinalizeSnapshot();
  const boost::filesystem::path &GetRootDir() const { return rootDir_; }
  const boost::filesystem::path &GetTempDir() const { return tmpDir_; }
  const boost::filesystem::path &GetFinalDir() const { return finalDir_; }
  boost::filesystem::path GetFilePath() const;
  boost::filesystem::path GetShrunkFilePath() const;
  boost::filesystem::path GetTempFilePath() const;
  Status CreateTempDir(bool must) { return createDir(tmpDir_, must); }
  Status RemoveTempDir(bool must) { return removeDir(tmpDir_, must); }
  Status RemoveFinalDir(bool must) { return removeDir(finalDir_, must); }
 private:
  Status createDir(const boost::filesystem::path &dir, bool must);
  Status removeDir(const boost::filesystem::path &dir, bool must);
  static std::mutex finalizeLock_;
  uint64_t index_;
  boost::filesystem::path rootDir_;  // specified by the upper layer, via SnapshotLocator
  boost::filesystem::path tmpDir_;   // temp snapshot directory, child of rootDir_
  boost::filesystem::path finalDir_; // final snapshot directory, child of rootDir
  boost::filesystem::path filePath_; // snapshot file path
};

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_SNAPSHOTENV_H_
