//
// Created by jason on 2019/12/21.
//

#include <iostream>
#include "library.h"
#include "utils/utils.h"
#include <gtest/gtest.h>

TEST(Transport, UnitTest)
{
  utils a;
  auto b = a.logger();
  std::cout << b->name() << std::endl;
  b->info("first log");
  ASSERT_EQ(1, 1);
}