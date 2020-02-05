//
// Created by jason on 2020/2/5.
//

#ifndef YCRT_SERVER_SNAPSHOTENV_H_
#define YCRT_SERVER_SNAPSHOTENV_H_

#include <stdint.h>
#include <string>
#include <functional>
#include <boost/filesystem.hpp>

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
  const boost::filesystem::path &GetRootDir() const { return rootDir_; }
  const boost::filesystem::path &GetTempDir() const { return tmpDir_; }
  const boost::filesystem::path &GetFinalDir() const { return finalDir_; }
  void RemoveTempDir() { remove(tmpDir_); }
  void RemoveFinalDir() { remove(finalDir_); }
 private:
  void createDir(const boost::filesystem::path &dir);
  void removeDir(const boost::filesystem::path &dir);
  uint64_t index_;
  boost::filesystem::path rootDir_;
  boost::filesystem::path tmpDir_;
  boost::filesystem::path finalDir_;
  boost::filesystem::path filePath_;
};

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_SNAPSHOTENV_H_
