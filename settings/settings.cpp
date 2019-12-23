//
// Created by jason on 2019/12/23.
//

#include "hard.h"
#include "soft.h"

namespace ycrt
{

namespace settings
{

// TODO
static soft parseSoftFrom(const char *filepath)
{
  return soft{};
}

static hard parseHardFrom(const char *filepath)
{
  return hard{};
}

soft &soft::ins()
{
  static soft cfg = parseSoftFrom("ycrt-soft-settings.json");
  return cfg;
}

hard &hard::ins()
{
  static hard cfg = parseHardFrom("ycrt-hard-settings.json");
  return cfg;
}

}

}

