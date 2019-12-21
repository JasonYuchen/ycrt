//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_UTILS_LOGGER_H_
#define YCRT_UTILS_LOGGER_H_

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace ycrt
{

class Logger {
 public:
  auto logger()
  {
    return spdlog::stdout_color_mt("abc");
  }
};

} // namespace ycrt

#endif //YCRT_UTILS_LOGGER_H_
