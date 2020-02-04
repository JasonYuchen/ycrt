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

TEST(Transport, Client)
{
  Log.GetLogger("transport")->set_level(spdlog::level::debug);
  auto nhConfig1 = NodeHostConfig();
  nhConfig1.DeploymentID = 10;
  nhConfig1.RaftAddress = "127.0.0.1:9009";
  nhConfig1.ListenAddress = "127.0.0.1:9009";
  auto handler = RaftMessageHandler();
  auto resolver1 = Nodes::New([](uint64_t){return 0;});
  resolver1->AddNode(1, 2, "127.0.0.1:9090");
  auto transport1 = Transport::New(nhConfig1, *resolver1, handler, [](uint64_t,uint64_t){return "no";}, 1);

  auto nhConfig2 = NodeHostConfig();
  nhConfig2.DeploymentID = 10;
  nhConfig2.RaftAddress = "127.0.0.1:9090";
  nhConfig2.ListenAddress = "127.0.0.1:9090";
  auto resolver2 = Nodes::New([](uint64_t){return 0;});
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
