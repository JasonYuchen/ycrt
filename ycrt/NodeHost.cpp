//
// Created by jason on 2019/12/21.
//

#include "transport/library.h"
#include "NodeHost.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

NodeHost::NodeHost()
{
  spdlog::stdout_color_mt("ycrt");
  spdlog::stdout_color_mt("config");
  spdlog::stdout_color_mt("logdb");
  spdlog::stdout_color_mt("raft");
  spdlog::stdout_color_mt("rsm");
  spdlog::stdout_color_mt("server");
  spdlog::stdout_color_mt("settings");
  spdlog::stdout_color_mt("test");
  spdlog::stdout_color_mt("transport");
}

void he()
{
  hello();
}