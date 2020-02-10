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
using namespace boost::asio;
using namespace boost::asio::chrono;
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
  int size = ::read(fd_, const_cast<char*>(buf.data()), buf.size());
  if (size < 0) {
    return {0, ErrorCode::FileSystem};
  }
  if (uint64_t(size) < buf.size()) {
    return {uint64_t(size), ErrorCode::ShortRead};
  }
  return uint64_t(size);
}

StatusWith<uint64_t> SnapshotChunkFile::ReadAt(std::string &buf, int64_t offset)
{
  int size = ::lseek(fd_, offset, SEEK_SET);
  if (size < 0) {
    return ErrorCode::FileSystem;
  }
  size = ::read(fd_, const_cast<char*>(buf.data()), buf.size());
  if (size < 0) {
    return {0, ErrorCode::FileSystem};
  }
  if (uint64_t(size) < buf.size()) {
    return {uint64_t(size), ErrorCode::ShortRead};
  }
  return uint64_t(size);
}

StatusWith<uint64_t> SnapshotChunkFile::Write(const std::string &buf)
{
  int size = ::write(fd_, buf.data(), buf.size());
  if (size < 0) {
    return {0, ErrorCode::FileSystem};
  }
  if (uint64_t(size) < buf.size()) {
    return {uint64_t(size), ErrorCode::ShortWrite};
  }
  return uint64_t(size);
}

Status SnapshotChunkFile::Sync()
{
  return SyncFd(fd_);
}

SnapshotChunkFile::~SnapshotChunkFile()
{
  if (::close(fd_) < 0) {
    Log.GetLogger("transport")->warn(
      "SnapshotChunkFile::~SnapshotChunkFile: "
      "failed to close fd={} due to error: {}",
      fd_, strerror(errno));
  }
  if (syncDir_) {
    SyncDir(dir_);
  }
}

// TODO: move to class SnapshotChunk
static string GetSnapshotKey(const pbSnapshotChunk &chunk)
{
  return fmt::format("{}:{}:{}",
    chunk.cluster_id(), chunk.node_id(), chunk.index());
}

unique_ptr<SnapshotChunkManager> SnapshotChunkManager::New(
  Transport &transport,
  io_context &io,
  function<string(uint64_t, uint64_t)> &&locator)
{
  unique_ptr<SnapshotChunkManager> manager(
    new SnapshotChunkManager(transport, io, std::move(locator)));
  return manager;
}

bool SnapshotChunkManager::AddChunk(pbSnapshotChunkSPtr chunk)
{
  // TODO: check BinVer
  if (chunk->deployment_id() != transport_.GetDeploymentID()) {
    log->error("SnapshotChunkManager::AddChunk: invalid deploymentID, "
               "expected={}, actual={}",
               transport_.GetDeploymentID(), chunk->deployment_id());
    return false;
  }
  string snapshotKey = GetSnapshotKey(*chunk);
  shared_ptr<mutex> snapshotLock = getSnapshotLock(snapshotKey);
  lock_guard<mutex> snapshotGuard(*snapshotLock);
  shared_ptr<track> t = onNewChunk(snapshotKey, chunk);
  // FIXME: consider try {} catch {} ...
  if (!t) {
    return false;
  }
  if (shouldUpdateValidator(*chunk)) {
    // TODO: validator
  }
  bool removed = nodeRemoved(*chunk).GetOrThrow();
  if (removed) {
    deleteTempChunkDir(*chunk);
    log->warn("SnapshotChunkManager::AddChunk: snapshot {}: "
              "node removed, ignored chunk {}", snapshotKey, chunk->chunk_id());
    return false;
  }
  Status saved = saveChunk(*chunk);
  if (!saved.IsOK()) {
    deleteTempChunkDir(*chunk);
    throw Error(saved.Code(), log,
      "SnapshotChunkManager::AddChunk: snapshot {}: "
      "failed to save chunk {}", snapshotKey, chunk->chunk_id());
  }
  if (chunk->chunk_id() + 1 == chunk->chunk_count()) {
    log->info("SnapshotChunkManager::AddChunk: snapshot {}: "
              "received last chunk {}", snapshotKey, chunk->chunk_id());
    // it is ok to remove tracked_[key] here
    // because we hold a sp to current track & lock
    deleteTrack(snapshotKey);
    if (validate_) {
      // TODO: validator
    }
    pbMessageBatchUPtr snapshotMsg = toMessageBatch(*chunk, t->extraFiles);
    Status finalized = finalizeSnapshot(*chunk, *snapshotMsg);
    if (!finalized.IsOK()) {
      deleteTempChunkDir(*chunk);
      if (finalized.Code() != ErrorCode::SnapshotOutOfDate) {
        throw Error(finalized.Code(), log,
          "SnapshotChunkManager::AddChunk: snapshot {}: "
          "failed to finalize", snapshotKey);
      }
      log->debug("SnapshotChunkManager::AddChunk: snapshot {}: "
                 "ignored out-of-date snapshot", snapshotKey);
      return false;
    }
    NodeInfo node{chunk->cluster_id(), chunk->node_id()};
    log->info("SnapshotChunkManager::AddChunk: "
              "{} received a snapshot, from={}, index={}, term={}",
              node, chunk->from(), chunk->index(), chunk->term());
    transport_.HandleRequest(std::move(snapshotMsg));
    transport_.HandleSnapshotConfirm(node, chunk->from());
  }
  return true;
}

void SnapshotChunkManager::RunTicker()
{
  gcTimer_.expires_from_now(seconds(1));
  gcTimer_.async_wait(
    [this](error_code ec)
  {
    if (gcTimer_.expiry() <= steady_timer::clock_type::now()) {
      uint64_t tick = currentTick_.fetch_add(1);
      log->debug("SnapshotChunkManager::RunTicker: tick={}", tick);
      if (tick % gcTick_ == 0) {
        log->info("SnapshotChunkManger::RunTicker: gc at tick={}", tick);
        gc();
      }
      RunTicker();
    }
  });
}

SnapshotChunkManager::SnapshotChunkManager(
  Transport &transport,
  io_context &io,
  function<string(uint64_t, uint64_t)> &&locator)
  : timeoutTick_(Soft::ins().SnapshotChunkTimeoutTick),
    gcTick_(Soft::ins().SnapshotGCTick),
    maxConcurrentSlot_(Soft::ins().MaxConcurrentStreamingSnapshot),
    log(Log.GetLogger("transport")),
    transport_(transport),
    gcTimer_(io),
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
    log->info("SnapshotChunkManager::onNewChunk: snapshot {}: "
              "received the first chunk", key);
    if (t != tracked_.end()) {
      log->warn("SnapshotChunkManager::onNewChunk: snapshot {}: "
                "removed unclaimed chunks", key);
      deleteTempChunkDir(*chunk);
      tracked_.erase(t);
    }
    if (tracked_.size() >= maxConcurrentSlot_) {
      log->error("SnapshotChunkManager::onNewChunk: snapshot {}: "
                 "exceeded max concurrent slot, dropped a chunk", key);
      return nullptr;
    }
    // TODO: auto validator = rsm.NewSnapshotValidator()
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
      log->warn("SnapshotChunkManager::onNewChunk: snapshot {}: "
                "ignored not tracked chunk {}",
                key, chunk->chunk_id());
      return nullptr;
    }
    if (t->second->nextChunk != chunk->chunk_id()) {
      log->warn("SnapshotChunkManager::onNewChunk: snapshot {}: "
                "ignored out-of-order chunk {}, expected={}",
                key, chunk->chunk_id(), t->second->nextChunk);
      return nullptr;
    }
    if (t->second->firstChunk->from() != chunk->from()) {
      log->warn("SnapshotChunkManager::onNewChunk: snapshot {}: "
                "ignored chunk {} from different node, from={}, expected={}",
                key, chunk->from(), t->second->firstChunk->from());
      return nullptr;
    }
    t->second->nextChunk = chunk->chunk_id() + 1;
  }
  if (chunk->file_chunk_id() == 0 && chunk->has_file_info()) {
    log->debug("SnapshotChunkManager::onNewChunk: snapshot {}: "
               "received first file chunk {}, filePath={}",
               key, chunk->chunk_id(), chunk->file_info().file_path());
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
      log->debug("SnapshotChunkManager::gc: snapshot {}: "
                 "timed out and removed", it->first);
      deleteTempChunkDir(*it->second->firstChunk);
      it = tracked_.erase(it);
    } else {
      it++;
    }
  }
}

void SnapshotChunkManager::deleteTrack(const string &key)
{
  lock_guard<mutex> guard(mutex_);
  tracked_.erase(key);
}

void SnapshotChunkManager::deleteTempChunkDir(const pbSnapshotChunk &chunk)
{
  getSnapshotEnv(chunk).RemoveTempDir(true);
}

bool SnapshotChunkManager::shouldUpdateValidator(
  const pbSnapshotChunk &chunk) const
{
  return validate_ && !chunk.has_file_info() && chunk.chunk_id() != 0;
}

StatusWith<bool> SnapshotChunkManager::nodeRemoved(
  const pbSnapshotChunk &chunk) const
{
  return IsDirMarkedAsDeleted(getSnapshotEnv(chunk).GetRootDir());
}

Status SnapshotChunkManager::saveChunk(const pbSnapshotChunk &chunk)
{
  auto env = getSnapshotEnv(chunk);
  if (chunk.chunk_id() == 0) {
    Status s = env.CreateTempDir();
    if (!s.IsOK()){
      return s;
    }
  }
  auto filename = path(chunk.filepath()).filename();
  auto filepath = env.GetTempDir() / filename;
  auto mode = chunk.file_chunk_id() == 0 ?
    SnapshotChunkFile::CREATE : SnapshotChunkFile::APPEND;
  SnapshotChunkFile file = SnapshotChunkFile::Open(filepath, mode);
  StatusWith<uint64_t> size = file.Write(chunk.data());
  if (!size.IsOK()) {
    return size.Code();
  }
  // FIXME: add methods IsLastChunk(), IsLastFileChunk() to pbSnapshotChunk
  if (chunk.chunk_id() + 1== chunk.chunk_count() ||
    chunk.file_chunk_id() + 1 == chunk.file_chunk_count()) {
    return file.Sync();
  }
  return ErrorCode::OK;
}

Status SnapshotChunkManager::finalizeSnapshot(
  const pbSnapshotChunk &chunk,
  const pbMessageBatch &msg)
{
  auto env = getSnapshotEnv(chunk);
  string data;
  if (!msg.requests(0).SerializeToString(&data)) {
    throw Error(ErrorCode::Other, "should not reach here");
  }
  return env.FinalizeSnapshot(data);
}

server::SnapshotEnv SnapshotChunkManager::getSnapshotEnv(
  const pbSnapshotChunk &chunk) const
{
  return server::SnapshotEnv(
    snapshotLocator_(chunk.cluster_id(), chunk.node_id()),
    chunk.index(),
    chunk.from(),
    server::SnapshotEnv::Receiving);
}

pbMessageBatchUPtr SnapshotChunkManager::toMessageBatch(
  const pbSnapshotChunk &chunk,
  const vector<pbSnapshotFileSPtr> &files) const
{
  auto env = getSnapshotEnv(chunk);
  auto m = make_unique<pbMessageBatch>();
  m->set_bin_ver(chunk.bin_ver());
  m->set_deployment_id(chunk.deployment_id());
  auto msg = m->add_requests();
  msg->set_type(raftpb::InstallSnapshot);
  msg->set_from(chunk.from());
  msg->set_to(chunk.node_id());
  msg->set_cluster_id(chunk.cluster_id());
  msg->set_allocated_snapshot(new pbSnapshot());
  msg->mutable_snapshot()->set_index(chunk.index());
  msg->mutable_snapshot()->set_term(chunk.term());
  msg->mutable_snapshot()->set_on_disk_index(chunk.on_disk_index());
  msg->mutable_snapshot()->set_allocated_membership(
    new pbMembership(chunk.membership()));
  msg->mutable_snapshot()->set_filepath(
    (env.GetFinalDir() / path(chunk.filepath()).filename()).string());
  msg->mutable_snapshot()->set_file_size(chunk.file_size());
  msg->mutable_snapshot()->set_witness(chunk.witness());
  for (auto &file : files) {
    auto *tmp = new pbSnapshotFile(*file);
    tmp->set_file_path(
      (env.GetFinalDir() / fmt::format("external-file-{}", file->file_id()))
      .string());
    msg->mutable_snapshot()->mutable_files()->AddAllocated(tmp);
  }
  return m;
}

} // namespace transport

} // namespace ycrt