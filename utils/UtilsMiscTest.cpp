//
// Created by jason on 2020/1/9.
//

#include <gtest/gtest.h>
#include <vector>
#include "Utils.h"

using namespace ycrt;
using namespace std;

TEST(Span, Basic)
{
  vector<int> ivec{1,2,3,4,5,6,7};
  Span<int> span1{&ivec[2], 3};
  ASSERT_EQ(span1[0], 3);
  ASSERT_EQ(span1[1], 4);
  ASSERT_EQ(span1[2], 5);
  ASSERT_EQ(span1.size(), 3);
  Span<int> span2 = span1.SubSpan(2, 1);
  ASSERT_EQ(span2[0], 5);
  ASSERT_EQ(span2.size(), 1);
}