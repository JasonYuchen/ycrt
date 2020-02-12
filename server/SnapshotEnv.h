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
using SnapshotLocator = std::function<std::string(NodeInfo node)>;

class SnapshotEnv {
 public:
  enum Mode { Snapshotting, Receiving };
  // root = SnapshotLocator(clusterID, nodeID)
  SnapshotEnv(const std::string &root, uint64_t index, uint64_t from, Mode mode);
  DEFAULT_COPY_MOVE_AND_ASSIGN(SnapshotEnv);

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
  void CreateTempDir() { createDir(tmpDir_); }

  // RemoveTempDir removes the temp snapshot directory.
  void RemoveTempDir() { removeDir(tmpDir_); }

  // RemoveFinalDir removes the final snapshot directory.
  void RemoveFinalDir() { removeDir(finalDir_); }

  // SaveSnapshotMetadata saves the metadata of the snapshot file.
  void SaveSnapshotMetadata(string_view metadata);

  bool HasFlagFile();
  void RemoveFlagFile();

  // FinalizeSnapshot finalizes the snapshot.
  // FIXME: FinalizeSnapshot argument proto.Message ?
  Status FinalizeSnapshot(string_view snapshotMsgRaw);
 private:
  void createDir(const boost::filesystem::path &dir);
  void removeDir(const boost::filesystem::path &dir);

  static std::mutex finalizeLock_;
  uint64_t index_;
  boost::filesystem::path rootDir_;  // specified by the upper layer, via SnapshotLocator
  boost::filesystem::path tmpDir_;   // temp snapshot directory, child of rootDir_
  boost::filesystem::path finalDir_; // final snapshot directory, child of rootDir
  boost::filesystem::path filePath_; // snapshot file path
};

using SnapshotEnvUPtr = std::unique_ptr<SnapshotEnv>;

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_SNAPSHOTENV_H_
