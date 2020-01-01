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
  Log.get("transport")->set_level(spdlog::level::debug);
  auto nhConfig1 = make_shared<NodeHostConfig>();
  nhConfig1->DeploymentID = 10;
  nhConfig1->RaftAddress = "127.0.0.1:9009";
  nhConfig1->ListenAddress = "127.0.0.1:9009";
  auto handler = make_shared<RaftMessageHandler>();
  auto resolver1 = Nodes::New([](uint64_t){return 0;});
  resolver1->addNode(1, 2, "127.0.0.1:9090");
  auto transport1 = Transport::New(nhConfig1, resolver1, handler, [](uint64_t,uint64_t){return "no";}, 1);
  transport1->start();

  auto nhConfig2 = make_shared<NodeHostConfig>();
  nhConfig2->DeploymentID = 10;
  nhConfig2->RaftAddress = "127.0.0.1:9090";
  nhConfig2->ListenAddress = "127.0.0.1:9090";
  auto resolver2 = Nodes::New([](uint64_t){return 0;});
  resolver2->addNode(1, 1, "127.0.0.1:9009");
  auto transport2 = Transport::New(nhConfig2, resolver2, handler, [](uint64_t,uint64_t){return "no";}, 1);
  transport2->start();
  Log.get("transport")->info("test start");

  MessageUPtr msg(new raftpb::Message());
  msg->set_cluster_id(1);
  msg->set_to(2);
  msg->set_from(1);
  transport1->asyncSendMessage(std::move(msg));

  int i;
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  Log.get("transport")->flush();
  //cin >> i;
  transport1->stop();
  transport2->stop();
  transport2.reset();
  transport1.reset();
  Log.get("transport")->info("release...");
  ASSERT_EQ(1, 1);
}

TEST(Transport, Server)
{

}