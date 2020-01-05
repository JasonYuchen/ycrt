//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_UTILS_LOGGER_H_
#define YCRT_UTILS_LOGGER_H_

#include <spdlog/sinks/stdout_color_sinks.h>
#include "utils/Types.h"
#include "utils/Error.h"

namespace ycrt
{

class Logger {
 public:
  static Logger &ins() {
    static Logger logger;
    return logger;
  }
  std::shared_ptr<spdlog::logger> GetLogger(const char *name) {
    assert(loggers_.find(name) != loggers_.end());
    return ins().loggers_[name];
  }
  // TODO: set pattern, level, sinks, etc...
 private:
  DISALLOW_COPY_MOVE_AND_ASSIGN(Logger);
  Logger() {
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    sink->set_pattern("%Y-%m-%d %H:%M:%S.%f %^%L%$ %n %t %v");
    loggers_["ycrt"] = std::make_shared<spdlog::logger>("ycrt", sink);
    loggers_["config"] = std::make_shared<spdlog::logger>("config", sink);
    loggers_["logdb"] = std::make_shared<spdlog::logger>("logdb", sink);
    loggers_["raft"] = std::make_shared<spdlog::logger>("raft", sink);
    loggers_["rsm"] = std::make_shared<spdlog::logger>("rsm", sink);
    loggers_["server"] = std::make_shared<spdlog::logger>("server", sink);
    loggers_["settings"] = std::make_shared<spdlog::logger>("settings", sink);
    loggers_["test"] = std::make_shared<spdlog::logger>("test", sink);
    loggers_["transport"] = std::make_shared<spdlog::logger>("transport", sink);
  }
  std::unordered_map<std::string, std::shared_ptr<spdlog::logger>> loggers_;
  static Logger instance_;
};

extern Logger &Log;

} // namespace ycrt



#endif //YCRT_UTILS_LOGGER_H_
