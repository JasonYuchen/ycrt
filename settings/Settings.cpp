//
// Created by jason on 2019/12/23.
//

#include "Hard.h"
#include "Soft.h"

namespace ycrt
{

namespace settings
{

// TODO
static Soft parseSoftFrom(const char *filepath)
{
  return Soft{};
}

static Hard parseHardFrom(const char *filepath)
{
  return Hard{};
}

Soft &Soft::ins()
{
  static Soft cfg = parseSoftFrom("ycrt-soft-settings.json");
  return cfg;
}

Hard &Hard::ins()
{
  static Hard cfg = parseHardFrom("ycrt-hard-settings.json");
  return cfg;
}

}

}

