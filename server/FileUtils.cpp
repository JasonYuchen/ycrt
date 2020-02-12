//
// Created by jason on 2020/2/6.
//

#include "FileUtils.h"

#ifdef _WIN32
#define fsync _commit
#endif

namespace ycrt
{

using namespace std;
using namespace boost::filesystem;
using boost::system::error_code;

static const char *deletedFlagFile = "DELETED";

//void logErrno(const char *desc)
//{
//  Log.GetLogger("server")->error("{}: {}", desc, strerror(errno));
//}

void SyncFd(int fd)
{
  if (::fsync(fd) < 0) {
    throw Error(ErrorCode::FileSystem,
      "SyncFd: fsync failed: {}", strerror(errno));
  }
}

void SyncDir(const path &dir)
{
  // FIXME: check the error
  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    throw Error(ErrorCode::FileSystem,
      "SyncDir: open failed: {}", strerror(errno));
  }
  if (::fsync(fd) < 0) {
    ::close(fd);
    throw Error(ErrorCode::FileSystem,
      "SyncDir: fsync failed: {}", strerror(errno));
  }
  if (::close(fd) < 0) {
    throw Error(ErrorCode::FileSystem,
      "SyncDir: close failed: {}", strerror(errno));
  }
}

void MarkDirAsDeleted(const path &dir, string_view content)
{
  CreateFlagFile(dir / deletedFlagFile, content);
}

bool IsDirMarkedAsDeleted(const path &dir)
{
  return exists(dir / deletedFlagFile);
}

void CreateFlagFile(const path &filePath, string_view content)
{
  int fd = ::open(filePath.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC);
  if (fd < 0) {
    throw Error(ErrorCode::FileSystem,
      "CreateFlagFile: open failed: {}", strerror(errno));
  }
  size_t size = ::write(fd, content.data(), content.size());
  if (size != content.size()) {
    ::close(fd);
    throw Error(ErrorCode::FileSystem,
      "CreateFlagFile: short write, expected {} actual {}: {}",
      content.size(), size, strerror(errno));
  }
  if (::fsync(fd) < 0) {
    ::close(fd);
    throw Error(ErrorCode::FileSystem,
      "CreateFlagFile: fsync failed: {}", strerror(errno));
  }
  if (::close(fd) < 0) {
    throw Error(ErrorCode::FileSystem,
      "CreateFlagFile: close failed: {}", strerror(errno));
  }
}

void RemoveFlagFile(const path &filePath)
{
  remove(filePath);
}

string GetFlagFileContent(const path &filePath)
{
  size_t len = file_size(filePath);
  int fd = ::open(filePath.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    throw Error(ErrorCode::FileSystem,
      "GetFlagFileContent: open failed: {}", strerror(errno));
  }
  std::string result;
  result.resize(len);
  size_t size = ::read(fd, const_cast<char*>(result.data()), len);
  if (size != len) {
    ::close(fd);
    throw Error(ErrorCode::FileSystem,
      "GetFlagFileContent: short read, expected {} actual {}: {}",
      len, size, strerror(errno));
  }
  return result;
}

} // namespace ycrt