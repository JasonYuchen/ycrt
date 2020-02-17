//
// Created by jason on 2020/2/12.
//

#include "Snapshotter.h"
#include "Manager.h"

namespace ycrt
{

namespace statemachine
{

using namespace std;
using namespace server;
using namespace boost::filesystem;

StatusWith<pair<pbSnapshotSPtr, SnapshotEnvUPtr>> Snapshotter::Save(
  Manager &manager,
  SnapshotMeta &meta)
{
  SnapshotEnvUPtr env = getSnapshotEnv(meta);
  env->CreateTempDir();
  SnapshotFileSet fileSet;
  path fp = env->GetTempFilePath();
  SnapshotWriter writer(fp, meta.CompressionType);
  StatusWith<bool> dummy = manager.SaveSnapshot(meta, writer, fileSet);
  if (!dummy.IsOK()) {
    return dummy.Code();
  }
  vector<pbSnapshotFileSPtr> files = fileSet.PrepareFiles(*env);
  auto snapshot = make_shared<pbSnapshot>();
  snapshot->set_cluster_id(node_.ClusterID);
  snapshot->set_filepath(env->GetFilePath().string());
  snapshot->set_allocated_membership(new pbMembership(*meta.Membership));
  snapshot->set_index(meta.Index);
  snapshot->set_term(meta.Term);
  snapshot->set_on_disk_index(meta.OnDiskIndex);
  for (auto &file : files) {
    snapshot->mutable_files()->AddAllocated(new pbSnapshotFile(*file));
  }
  snapshot->set_dummy(dummy.GetOrThrow());
  snapshot->set_type(meta.StateMachineType);
  return {{std::move(snapshot), std::move(env)}};
}

Status Snapshotter::Load(
  Manager &manager,
  SessionManager &sessions,
  const pbSnapshot &snapshot)
{
  path fp = getSnapshotEnv(snapshot.index())->GetFilePath();
  vector<SnapshotFile> files;
  for (auto &file : snapshot.files()) {
    files.push_back({});
    files.back().FileID = file.file_id();
    files.back().FilePath = file.file_path();
    files.back().Metadata = file.metadata();
  }
  // TODO: compression type, size of sessions are encoded in the file
  SnapshotReader reader(fp, NoCompression);
  string sessionBuf;
  sessionBuf.resize(0/*FIXME: session size*/);
  reader.Read(sessionBuf);
  uint64_t readBytes = sessions.LoadSessions(sessionBuf);
  if (readBytes != sessionBuf.size()) {
    return ErrorCode::InvalidSession;
  }
  Status s = manager.RecoverFromSnapshot(reader, files);
  if (!s.IsOK()) {
    return s;
  }
  return ErrorCode::OK;
}

SnapshotEnvUPtr Snapshotter::getSnapshotEnv(const SnapshotMeta &meta)
{
  if (meta.Request.IsExported()) {
    if (meta.Request.Path.empty()) {
      throw Error(ErrorCode::SnapshotEnvError, log,
        "Snapshotter::getSnapshotEnv: export path is empty");
    }
    return make_unique<SnapshotEnv>(
      locator_(node_),
      meta.Index,
      node_.NodeID,
      SnapshotEnv::Snapshotting);
  }
  return getSnapshotEnv(meta.Index);
}

SnapshotEnvUPtr Snapshotter::getSnapshotEnv(uint64_t index)
{
  return make_unique<SnapshotEnv>(
    locator_(node_),
    index,
    node_.NodeID,
    SnapshotEnv::Snapshotting);
}

} // namespace statemachine

} // namespace ycrt