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

// root SnapshotLocator(clusterID, nodeID)
using SnapshotLocator = std::function<std::string(uint64_t, uint64_t)>;

class SnapshotEnv {
 public:
  enum Mode { Snapshotting, Receiving };
  // root = SnapshotLocator(clusterID, nodeID)
  SnapshotEnv(const std::string &root, uint64_t index, uint64_t from, Mode mode);

  // GetRootDir returns the root directory. The temp and final snapshot
  // directories are children of the root directory.
  const boost::filesystem::path &GetRootDir() const { return rootDir_; }

  // GetTempDir returns the temp snapshot directory.
  const boost::filesystem::path &GetTempDir() const { return tmpDir_; }

  // GetFinalDir returns the final snapshot directory.
  const boost::filesystem::path &GetFinalDir() const { return finalDir_; }

  // GetFilePath returns the snapshot file path.
  boost::filesystem::path GetFilePath() const;

  // GetShrunkFilePath returns the file path of the shrunk snapshot.
  boost::filesystem::path GetShrunkFilePath() const;

  // GetTempFilePath returns the temp snapshot file path.
  boost::filesystem::path GetTempFilePath() const;

  // CreateTempDir creates the temp snapshot directory.
  Status CreateTempDir() { return createDir(tmpDir_, false); }

  // RemoveTempDir removes the temp snapshot directory.
  Status RemoveTempDir(bool must) { return removeDir(tmpDir_, must); }

  // RemoveFinalDir removes the final snapshot directory.
  Status RemoveFinalDir(bool must) { return removeDir(finalDir_, must); }

  // SaveSnapshotMetadata saves the metadata of the snapshot file.
  Status SaveSnapshotMetadata(string_view metadata);

  bool HasFlagFile();
  Status RemoveFlagFile();

  // FinalizeSnapshot finalizes the snapshot.
  // FIXME: FinalizeSnapshot argument proto.Message ?
  Status FinalizeSnapshot(string_view snapshotMsgRaw);
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
