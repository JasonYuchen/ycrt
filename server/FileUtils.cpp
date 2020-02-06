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

static const char *deletedFileFlag = "DELETED";

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
  return CreateFlagFile(dir / deletedFileFlag, content);
}

StatusWith<bool> IsDirMarkedAsDeleted(const path &dir)
{
  error_code ec;
  bool existing = exists(dir / deletedFileFlag, ec);
  if (ec) {
    return ErrorCode::FileSystem;
  } else {
    return existing;
  }
}


// TODO: use C functions
Status CreateFlagFile(const path &filePath, string_view content)
{
//  {
//    fstream f(filePath, fstream::out | fstream::binary);
//    if (!f) {
//      logErrno("CreateFlagFile fstream");
//      return ErrorCode::FileSystem;
//    }
//    f << content.size() << content;
//    //Sync(f);
//  }
//  return SyncDir(filePath.parent_path());
}

// TODO: use C functions
StatusWith<string> GetFlagFileContent(const path &filePath)
{
//  fstream f(filePath, fstream::in | fstream::binary);
//  if (!f) {
//    logErrno("GetFlagFileContent fstream");
//    return ErrorCode::FileSystem;
//  }
//  size_t len;
//  f >> len;
//  string content((istream_iterator<char>(f)), istream_iterator<char>());
//  if (content.size() != len) {
//    return ErrorCode::FileSystem;
//  }
//  return content;
}

} // namespace ycrt