//
// Created by jason on 2019/12/31.
//

#include "SnapshotChunkManager.h"
#include "settings/Soft.h"
#include "Transport.h"

namespace ycrt
{

namespace transport
{

using namespace std;
using namespace settings;
using namespace boost::filesystem;

SnapshotChunkFile SnapshotChunkFile::Open(path file, Mode mode)
{
  if (mode == CREATE) {
    int fd = ::open(file.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC);
    if (fd < 0) {
      throw Error(ErrorCode::FileSystem, strerror(errno));
    }
    return SnapshotChunkFile(fd, true, file.parent_path());
  } else if (mode == READ) {
    int fd = ::open(file.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
      throw Error(ErrorCode::FileSystem, strerror(errno));
    }
    return SnapshotChunkFile(fd, false, {});
  } else if (mode == APPEND) {
    int fd = ::open(file.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
    if (fd < 0) {
      throw Error(ErrorCode::FileSystem, strerror(errno));
    }
    return SnapshotChunkFile(fd, false, {});
  } else {
    throw Error(ErrorCode::Other, "unknown snapshot chunk file open mode");
  }
}

SnapshotChunkFile::SnapshotChunkFile(int fd, bool syncDir, path dir)
  : fd_(fd), syncDir_(syncDir), dir_(std::move(dir))
{
}

StatusWith<uint64_t> SnapshotChunkFile::Read(std::string &buf)
{
  size_t size = ::read(fd_, const_cast<char*>(buf.data()), buf.size());
  if (size != buf.size()) {
    return {size, ErrorCode::FileSystem};
  }
  return size;
}

StatusWith<uint64_t> SnapshotChunkFile::ReadAt(std::string &buf, int64_t offset)
{
  size_t size = ::lseek(fd_, offset, SEEK_SET);
  if (size < 0) {
    return ErrorCode::FileSystem;
  }
  size = ::read(fd_, const_cast<char*>(buf.data()), buf.size());
  if (size != buf.size()) {
    return {size, ErrorCode::FileSystem};
  }
  return size;
}

StatusWith<uint64_t> SnapshotChunkFile::Write(const std::string &buf)
{
  size_t size = ::write(fd_, buf.data(), buf.size());
  if (size != buf.size()) {
    return {size, ErrorCode::FileSystem};
  }
  return size;
}

Status SnapshotChunkFile::Sync()
{
  return SyncFd(fd_);
}

SnapshotChunkFile::~SnapshotChunkFile()
{
  if (::close(fd_) < 0) {
    Log.GetLogger("transport")->warn(
      "failed to close fd={0} in chunk file dtor due to {1}",
      fd_, strerror(errno));
  }
  if (syncDir_) {
    SyncDir(dir_);
  }
}

// TODO: move to class SnapshotChunk
static string GetSnapshotKey(const pbSnapshotChunk &chunk)
{
  return fmt::format("{0}:{1}:{2}",
    chunk.cluster_id(), chunk.node_id(), chunk.index());
}

unique_ptr<SnapshotChunkManager> SnapshotChunkManager::New(
  Transport &transport,
  function<string(uint64_t, uint64_t)> &&locator)
{
  unique_ptr<SnapshotChunkManager> manager(
    new SnapshotChunkManager(transport, std::move(locator)));
  return manager;
}

bool SnapshotChunkManager::AddChunk(pbSnapshotChunkSPtr chunk)
{
  // FIXME: check BinVer
  if (chunk->deployment_id() != transport_.GetDeploymentID()) {
    log->error("SnapshotChunkManager::AddChunk: invalid deploymentID, "
               "expected={0}, actual={1}",
               transport_.GetDeploymentID(), chunk->deployment_id());
    return false;
  }
  string snapshotKey = GetSnapshotKey(*chunk);
  shared_ptr<mutex> snapshotLock = getSnapshotLock(snapshotKey);
  lock_guard<mutex> snapshotGuard(*snapshotLock);
  shared_ptr<track> t = onNewChunk(snapshotKey, chunk);
  if (!t) {
    return false;
  }
  if (shouldUpdateValidator(*chunk)) {
    // FIXME: validator
  }
  if (nodeRemoved(*chunk)) {
    deleteTempChunkDir(*chunk);
    log->warn("SnapshotChunkManager::AddChunk: "
              "node removed, ignored chunk, key={0}", snapshotKey);
    return false;
  }
  // TODO
  return true;
}

void SnapshotChunkManager::Tick()
{
  uint64_t tick = currentTick_.fetch_add(1);
  if (tick % gcTick_ == 0) {
    gc();
  }
}

SnapshotChunkManager::SnapshotChunkManager(
  Transport &transport_,
  function<string(uint64_t, uint64_t)> &&locator)
  : timeoutTick_(Soft::ins().SnapshotChunkTimeoutTick),
    gcTick_(Soft::ins().SnapshotGCTick),
    maxConcurrentSlot_(Soft::ins().MaxConcurrentStreamingSnapshot),
    log(Log.GetLogger("transport")),
    transport_(transport_),
    currentTick_(0),
    validate_(true),
    snapshotLocator_(std::move(locator)),
    mutex_(),
    tracked_(),
    locks_()
{
}

shared_ptr<mutex> SnapshotChunkManager::getSnapshotLock(
  const string &snapshotKey)
{
  lock_guard<mutex> guard(mutex_);
  if (locks_.find(snapshotKey) == locks_.end()) {
    locks_.emplace(snapshotKey, make_shared<mutex>());
  }
  return locks_[snapshotKey];
}

shared_ptr<SnapshotChunkManager::track> SnapshotChunkManager::onNewChunk(
  const string &key,
  pbSnapshotChunkSPtr chunk)
{
  lock_guard<mutex> guard(mutex_);
  auto t = tracked_.find(key);
  if (chunk->chunk_id() == 0) {
    log->info("received the first chunk of a snapshot, key={0}", key);
    if (t != tracked_.end()) {
      log->warn("removing unclaimed chunks, key={0}", key);
      deleteTempChunkDir(*chunk);
      tracked_.erase(t);
    }
    if (tracked_.size() >= maxConcurrentSlot_) {
      log->error("max slot count reached, dropped a chunk, key={0}", key);
      return nullptr;
    }
    // FIXME: auto validator = rsm.NewSnapshotValidator()
    //  if (validate && !chunk->HasFileInfo) {
    //    .....
    //  }
    auto newTrack = make_shared<track>();
    newTrack->firstChunk = chunk;
    // newTrack->validator = validator;
    newTrack->nextChunk = 1;
    newTrack->tick = 0;
    tracked_.emplace(key, std::move(newTrack));
    t = tracked_.find(key);
  } else {
    if (t == tracked_.end()) {
      log->warn("ignored a not tracked chunk, key={0}, id={1}",
        key, chunk->chunk_id());
      return nullptr;
    }
    if (t->second->nextChunk != chunk->chunk_id()) {
      log->warn("ignored an out of order chunk, key={0}, id={1}, expected={2}",
        key, chunk->chunk_id(), t->second->nextChunk);
      return nullptr;
    }
    if (t->second->firstChunk->from() != chunk->from()) {
      log->warn("ignored a chunk, key={0}, from={1}, expected={2}",
        key, chunk->from(), t->second->firstChunk->from());
      return nullptr;
    }
    t->second->nextChunk = chunk->chunk_id() + 1;
  }
  if (chunk->file_chunk_id() == 0 && chunk->has_file_info()) {
    t->second->extraFiles.emplace_back(
      make_shared<pbSnapshotFile>(chunk->file_info()));
  }
  t->second->tick = currentTick_;
  return t->second;
}

void SnapshotChunkManager::gc()
{
  lock_guard<mutex> guard(mutex_);
  uint64_t tick = currentTick_;
  for (auto it = tracked_.begin(); it != tracked_.end();) {
    if (tick - it->second->tick >= timeoutTick_) {
      deleteTempChunkDir(*it->second->firstChunk);
      it = tracked_.erase(it);
    } else {
      it++;
    }
  }
}

void SnapshotChunkManager::deleteTempChunkDir(const pbSnapshotChunk &chunk)
{
  getSnapshotEnv(chunk).RemoveTempDir(true);
}

bool SnapshotChunkManager::shouldUpdateValidator(const pbSnapshotChunk &chunk)
{
  // TODO
  return false;
}

bool SnapshotChunkManager::nodeRemoved(const pbSnapshotChunk &chunk)
{
  return IsDirMarkedAsDeleted(getSnapshotEnv(chunk).GetRootDir()).GetOrThrow();
}

Status SnapshotChunkManager::saveChunk(const pbSnapshotChunk &chunk)
{
  server::SnapshotEnv env = getSnapshotEnv(chunk);
  if (chunk.chunk_id() == 0) {
    Status s = env.CreateTempDir();
    if (!s.IsOK()){
      return s;
    }
  }
  auto filename = path(chunk.filepath()).filename();
  auto file = env.GetTempDir() / filename;
  // TODO
  return ErrorCode::OK;
}

server::SnapshotEnv SnapshotChunkManager::getSnapshotEnv(
  const pbSnapshotChunk &chunk)
{
  return server::SnapshotEnv(
    snapshotLocator_(chunk.cluster_id(), chunk.node_id()),
    chunk.index(),
    chunk.from(),
    server::SnapshotEnv::Receiving);
}

} // namespace transport

} // namespace ycrt