//
// Created by jason on 2020/2/11.
//

#include "SnapshotIO.h"
#include "Manager.h"

namespace ycrt
{

namespace statemachine
{

using namespace std;
using namespace server;
using namespace boost::filesystem;
using boost::system::error_code;

SnapshotWriter::SnapshotWriter(path path, CompressionType type)
  : fd_(-1), fp_(std::move(path)), writtenBytes_(0)
{
  if ((fd_ = ::open(fp_.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC)) < 0) {
    throw Error(ErrorCode::FileSystem,
      "SnapshotWriter::SnapshotWriter: open failed: {}", strerror(errno));
  }
}

uint64_t SnapshotWriter::Write(string_view content)
{
  int size = ::write(fd_, content.data(), content.size());
  if (size < 0) {
    throw Error(ErrorCode::FileSystem,
      "SnapshotWriter::Write: write failed: {}", strerror(errno));
  }
  if (size < content.size()) {
    throw Error(ErrorCode::FileSystem,
      "SnapshotWriter::Write: short write, expected {} actual {}: {}",
      content.size(), size, strerror(errno));
  }
  writtenBytes_ += size;
  return size;
}

uint64_t SnapshotWriter::GetPayloadSize() const
{
  return writtenBytes_;
}

uint64_t SnapshotWriter::GetPayloadChecksum() const
{
  return writtenBytes_;
}

SnapshotWriter::~SnapshotWriter()
{
  try {
    SyncFd(fd_);
    if (::close(fd_) < 0) {
      throw Error(ErrorCode::FileSystem, "close failed: {}", strerror(errno));
    }
    SyncDir(fp_.parent_path());
  } catch (Error &e) {
    Log.GetLogger("statemachine")->critical(
      "SnapshotWriter::~SnapshotWriter: {}", e.what());
  }
}

void SnapshotFileSet::AddFile(uint64_t fileID, const path &path, string metadata)
{
  if (ids_.find(fileID) != ids_.end()) {
    throw Error(ErrorCode::Other, "fileID {} already exists", fileID);
  }
  ids_.insert(fileID);
  auto file = make_shared<pbSnapshotFile>();
  file->set_file_id(fileID);
  file->set_file_path(path.string());
  file->set_metadata(std::move(metadata));
  files_.push_back(std::move(file));
}

size_t SnapshotFileSet::Size() const
{
  return files_.size();
}

pbSnapshotFileSPtr SnapshotFileSet::GetFile(uint64_t index)
{
  return files_[index];
}

vector<pbSnapshotFileSPtr> SnapshotFileSet::PrepareFiles(
  const server::SnapshotEnv &env)
{
  for (auto &file : files_) {
    string fn = fmt::format("external-file-{}", file->file_id());
    path fp = env.GetTempDir() / fn;
    create_hard_link(file->file_path(), fp);
    uint64_t fileSize = file_size(fp);
    if (is_directory(fp)) {
      throw Error(ErrorCode::FileSystem, "the extra file is a directory");
    }
    if (fileSize == 0) {
      throw Error(ErrorCode::FileSystem, "the extra file is empty");
    }
    file->set_file_path((env.GetFinalDir() / fn).string());
    file->set_file_size(fileSize);
  }
  std::vector<pbSnapshotFileSPtr> files = std::move(files_);
  return files;
}

} // namespace statemachine

} // namespace ycrt
