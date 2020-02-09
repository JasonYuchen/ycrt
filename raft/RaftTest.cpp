//
// Created by jason on 2020/1/9.
//

#include <gtest/gtest.h>
#include "Raft.h"

using namespace ycrt;

TEST(Raft, Core)
{
  int i = 9;
  throw Error(ErrorCode::UnexpectedRaftState, "ok? {}:{}", 5, i);
  ASSERT_TRUE(1);
}