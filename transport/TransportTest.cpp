//
// Created by jason on 2019/12/21.
//

#include <iostream>
#include <gtest/gtest.h>
#include "utils/Utils.h"
#include "Transport.h"

using namespace std;
using namespace ycrt;
using namespace ycrt::transport;
using namespace boost::filesystem;

TEST(Transport, Client)
{
  Log.GetLogger("transport")->set_level(spdlog::level::debug);
  auto nhConfig1 = NodeHostConfig();
  nhConfig1.DeploymentID = 10;
  nhConfig1.RaftAddress = "127.0.0.1:9009";
  nhConfig1.ListenAddress = "127.0.0.1:9009";
  auto handler = RaftMessageHandler();
  auto resolver1 = NodeResolver::New([](uint64_t){return 0;});
  resolver1->AddNode(NodeInfo{1, 2}, "127.0.0.1:9090");
  auto transport1 = Transport::New(nhConfig1, *resolver1, handler, [](uint64_t,uint64_t){return "no";}, 1);

  auto nhConfig2 = NodeHostConfig();
  nhConfig2.DeploymentID = 10;
  nhConfig2.RaftAddress = "127.0.0.1:9090";
  nhConfig2.ListenAddress = "127.0.0.1:9090";
  auto resolver2 = NodeResolver::New([](uint64_t){return 0;});
  auto transport2 = Transport::New(nhConfig2, *resolver2, handler, [](uint64_t,uint64_t){return "no";}, 1);
  Log.GetLogger("transport")->info("test start");

  for (int i = 0; i < 20; ++i) {
    pbMessageUPtr msg1(new raftpb::Message());
    msg1->set_cluster_id(1);
    msg1->set_to(2);
    msg1->set_from(1);
    transport1->AsyncSendMessage(std::move(msg1));
  }

  pbMessageUPtr msg2(new raftpb::Message());
  msg2->set_cluster_id(1);
  msg2->set_to(3);
  msg2->set_from(1);
  transport1->AsyncSendMessage(std::move(msg2));
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  Log.GetLogger("transport")->flush();
  transport1->Stop();
  transport2->Stop();
  transport1.reset();
  transport2.reset();
  Log.GetLogger("transport")->info("release...");
  ASSERT_EQ(1, 1);
}

TEST(Transport, AsyncSendSnapshotWith1Chunks)
{
  Log.GetLogger("transport")->set_level(spdlog::level::debug);
  auto nhConfig1 = NodeHostConfig();
  nhConfig1.DeploymentID = 10;
  nhConfig1.RaftAddress = "127.0.0.1:9009";
  nhConfig1.ListenAddress = "127.0.0.1:9009";
  auto locator1 = [](NodeInfo){return "test_snap_dir_1";};
  remove_all("test_snap_dir_1");
  remove_all("test_snap_dir_2");
  create_directory("test_snap_dir_1");
  create_directory("test_snap_dir_2");
  string testPayload;
  testPayload.insert(0, 1 * 1024 * 1024, 'a');
  Status s = CreateFlagFile(path("test_snap_dir_1") / "snap", testPayload);
  s.IsOKOrThrow();
  auto handler = RaftMessageHandler();
  auto resolver1 = NodeResolver::New([](uint64_t){return 0;});
  resolver1->AddNode(NodeInfo{1, 2}, "127.0.0.1:9090");
  auto transport1 = Transport::New(nhConfig1, *resolver1, handler, locator1, 1);

  auto nhConfig2 = NodeHostConfig();
  nhConfig2.DeploymentID = 10;
  nhConfig2.RaftAddress = "127.0.0.1:9090";
  nhConfig2.ListenAddress = "127.0.0.1:9090";
  auto locator2 = [](NodeInfo){return "test_snap_dir_2";};
  auto resolver2 = NodeResolver::New([](uint64_t){return 0;});
  auto transport2 = Transport::New(nhConfig2, *resolver2, handler, locator2, 1);
  Log.GetLogger("transport")->info("test start");
  for (int i = 0; i < 1; ++i) {
    pbMessageUPtr msg1(new raftpb::Message());
    msg1->set_type(raftpb::InstallSnapshot);
    msg1->set_cluster_id(1);
    msg1->set_to(2);
    msg1->set_from(1);
    msg1->set_allocated_snapshot(new pbSnapshot());
    auto *sp = msg1->mutable_snapshot();
    sp->set_type(raftpb::RegularStateMachine);
    sp->set_index(5);
    sp->set_term(6);
    sp->set_on_disk_index(7);
    sp->set_cluster_id(1);
    sp->set_filepath((path("test_snap_dir_1") / "snap").string());
    sp->set_file_size(8 + testPayload.size()); // check, header=8 bytes (uint64_t), payload=4 bytes ("test")
    sp->set_allocated_membership(new pbMembership());
    sp->set_witness(false);
    transport1->AsyncSendSnapshot(std::move(msg1));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  transport1->Stop();
  transport2->Stop();
  transport1.reset();
  transport2.reset();
  ASSERT_EQ(1, 1);
}

TEST(Transport, AsyncSendSnapshotWith2Chunks)
{
  Log.GetLogger("transport")->set_level(spdlog::level::debug);
  auto nhConfig1 = NodeHostConfig();
  nhConfig1.DeploymentID = 10;
  nhConfig1.RaftAddress = "127.0.0.1:9009";
  nhConfig1.ListenAddress = "127.0.0.1:9009";
  auto locator1 = [](NodeInfo){return "test_snap_dir_1";};
  remove_all("test_snap_dir_1");
  remove_all("test_snap_dir_2");
  create_directory("test_snap_dir_1");
  create_directory("test_snap_dir_2");
  string testPayload;
  testPayload.insert(0, 3 * 1024 * 1024, 'a');
  Status s = CreateFlagFile(path("test_snap_dir_1") / "snap", testPayload);
  s.IsOKOrThrow();
  auto handler = RaftMessageHandler();
  auto resolver1 = NodeResolver::New([](uint64_t){return 0;});
  resolver1->AddNode(NodeInfo{1, 2}, "127.0.0.1:9090");
  auto transport1 = Transport::New(nhConfig1, *resolver1, handler, locator1, 1);

  auto nhConfig2 = NodeHostConfig();
  nhConfig2.DeploymentID = 10;
  nhConfig2.RaftAddress = "127.0.0.1:9090";
  nhConfig2.ListenAddress = "127.0.0.1:9090";
  auto locator2 = [](NodeInfo){return "test_snap_dir_2";};
  auto resolver2 = NodeResolver::New([](uint64_t){return 0;});
  auto transport2 = Transport::New(nhConfig2, *resolver2, handler, locator2, 1);
  Log.GetLogger("transport")->info("test start");
  for (int i = 0; i < 1; ++i) {
    pbMessageUPtr msg1(new raftpb::Message());
    msg1->set_type(raftpb::InstallSnapshot);
    msg1->set_cluster_id(1);
    msg1->set_to(2);
    msg1->set_from(1);
    msg1->set_allocated_snapshot(new pbSnapshot());
    auto *sp = msg1->mutable_snapshot();
    sp->set_type(raftpb::RegularStateMachine);
    sp->set_index(5);
    sp->set_term(6);
    sp->set_on_disk_index(7);
    sp->set_cluster_id(1);
    sp->set_filepath((path("test_snap_dir_1") / "snap").string());
    sp->set_file_size(8 + testPayload.size()); // check, header=8 bytes (uint64_t), payload=4 bytes ("test")
    sp->set_allocated_membership(new pbMembership());
    sp->set_witness(false);
    transport1->AsyncSendSnapshot(std::move(msg1));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  transport1->Stop();
  transport2->Stop();
  transport1.reset();
  transport2.reset();
  ASSERT_EQ(1, 1);
}

TEST(Transport, SnapshotChunkManagerGC)
{
  Log.SetErrorHandler([](const std::string &msg){
    std::cerr << "msg" << std::endl;
    std::abort();
  });
  Log.GetLogger("transport")->set_level(spdlog::level::debug);
  auto locator = [](NodeInfo){return "test_snap_dir_2";};
  boost::asio::io_service io;
  boost::asio::io_service::work work(io);
  thread thread([&io](){io.run();});
  int t;
  auto manager = SnapshotChunkManager::New((Transport&)(t), io, locator);
  manager->RunTicker();
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  io.stop();
  thread.join();
  Log.GetLogger("transport")->info("release... {}");
  Log.GetLogger("transport")->info("release... {} {}", "ok");
  Log.GetLogger("transport")->info("release... ", "ok");
}