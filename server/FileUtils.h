//
// Created by jason on 2020/2/5.
//

#ifndef YCRT_SERVER_FILEUTILS_H_
#define YCRT_SERVER_FILEUTILS_H_

#include <fcntl.h>

#include <boost/filesystem.hpp>

namespace ycrt
{

// TODO: fsync is a must

inline void SyncDir(const boost::filesystem::path &dir)
{
  if (boost::filesystem::is_regular_file(dir)) {
    throw Error(ErrorCode::Other);
  }
  int fd = ::open(dir.c_str(), O_RDONLY);
  ::fsync(fd);
  ::close(fd);
}

} // namespace ycrt

#endif //YCRT_SERVER_FILEUTILS_H_
