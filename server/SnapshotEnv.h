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
 private:
  uint64_t index_;
  boost::filesystem::path rootDir_;
  //std::string rootDir_;
  boost::filesystem::path tmpDir_;
  boost::filesystem::path finalDir_;
  boost::filesystem::path filePath_;
};

} // namespace server

} // namespace ycrt

#endif //YCRT_SERVER_SNAPSHOTENV_H_
