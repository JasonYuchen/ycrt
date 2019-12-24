//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_UTILS_ERROR_H_
#define YCRT_UTILS_ERROR_H_

#include <string>
#include <stdexcept>

namespace ycrt
{

class Error : public std::runtime_error {
 public:
  Error(const std::string &what) : std::runtime_error(what) {}
 private:

};

} // namespace ycrt

#endif //YCRT_UTILS_ERROR_H_
