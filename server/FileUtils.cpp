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

static void logErrno(const char *desc)
{
  Log.GetLogger("server")->error("{0}: {1}", desc, strerror(errno));
}

Status SyncDir(const path &dir)
{
  // FIXME: check the error
  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    logErrno("SyncDir open");
    return ErrorCode::FileSystem;
  }
  if (::fsync(fd) < 0) {
    ::close(fd);
    logErrno("SyncDir fsync");
    return ErrorCode::FileSystem;
  }
  if (::close(fd) < 0) {
    logErrno("SyncDir close");
    return ErrorCode::FileSystem;
  }
  return ErrorCode::OK;
}

//Status Sync(fstream &fs)
//{
//  // FIXME: ugly hacking
//  // fs.sync();
//  if (!fs) {
//    return ErrorCode::FileSystem;
//  }
//  class F : public boost::filesystem::filebuf {
//   public:
//    int fd() { return _M_file.fd(); }
//  };
//  int fd = static_cast<F&>(*fs.rdbuf()).fd();
//  if(fd < 0) {
//    logErrno("Sync fd");
//    return ErrorCode::FileSystem;
//  }
//  if (::fsync(fd) < 0) {
//    logErrno("Sync fsync");
//    return ErrorCode::FileSystem;
//  }
//  return ErrorCode::OK;
//}

Status MarkDirAsDeleted(const path &dir, string_view content)
{
  return CreateFlagFile(dir / deletedFlagFile, content);
}

StatusWith<bool> IsDirMarkedAsDeleted(const path &dir)
{
  error_code ec;
  bool existing = exists(dir / deletedFlagFile, ec);
  if (ec) {
    return ErrorCode::FileSystem;
  } else {
    return existing;
  }
}


// TODO: use C functions
Status CreateFlagFile(const path &filePath, string_view content)
{
  int fd = ::open(filePath.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC);
  if (fd < 0) {
    logErrno("CreateFlagFile open");
    return ErrorCode::FileSystem;
  }
  size_t len = content.size();
  size_t size = ::write(fd, &len, sizeof(size_t));
  if (size != sizeof(size_t)) {
    ::close(fd);
    return ErrorCode::FileSystem;
  }
  size = ::write(fd, content.data(), len);
  if (size != len) {
    ::close(fd);
    return ErrorCode::FileSystem;
  }
  if (::fsync(fd) < 0) {
    logErrno("CreateFlagFile fsync");
    ::close(fd);
    return ErrorCode::FileSystem;
  }
  if (::close(fd) < 0) {
    logErrno("CreateFlagFile close");
    return ErrorCode::FileSystem;
  }
  return ErrorCode::OK;
}

Status RemoveFlagFile(const path &filePath)
{
  error_code ec;
  bool done = remove(filePath, ec);
  if (!done || ec) {
    return ErrorCode::FileSystem;
  }
  return ErrorCode::OK;
}

// TODO: use C functions
StatusWith<string> GetFlagFileContent(const path &filePath)
{
  int fd = ::open(filePath.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    logErrno("GetFlagFileContent open");
    return ErrorCode::FileSystem;
  }
  size_t len;
  std::string result;
  size_t size = ::read(fd, &len, sizeof(size_t));
  if (size != sizeof(size_t)) {
    ::close(fd);
    return ErrorCode::FileSystem;
  }
  result.resize(len);
  size = ::read(fd, const_cast<char*>(result.data()), len);
  if (size != len) {
    ::close(fd);
    return ErrorCode::FileSystem;
  }
  return result;
}

} // namespace ycrt