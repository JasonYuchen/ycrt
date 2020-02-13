//
// Created by jason on 2020/1/9.
//

#include <gtest/gtest.h>
#include <vector>
#include "Utils.h"
#include "server/FileUtils.h"

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

// fsync maybe problematic in WSL
// see https://askubuntu.com/questions/1121820/postgresql-not-starting
TEST(File, Fsync)
{
  boost::filesystem::path dir("abc");
  boost::filesystem::path file(dir / "text.txt");
  boost::system::error_code ec;
  create_directory(dir, ec);
  boost::filesystem::fstream f(file, boost::filesystem::fstream::out);
  ASSERT_TRUE(exists(dir));
  ASSERT_TRUE(exists(file));
  int fd = ::open(file.c_str(), O_RDWR);
  int dd = ::open("", O_RDONLY);
  ::write(fd, "abcd", 4);
  int ret1 = fsync(fd);
  f.sync();
  if (ret1 < 0) {
    cout << strerror(errno) << endl;
  } else {
    ::close(fd);
  }
  int ret2 = fsync(dd);
  if (ret2 < 0) {
    cout << strerror(errno) << endl;
  } else {
    ::close(dd);
  }
}

TEST(File, Read)
{
  boost::filesystem::path dir("abc");
  boost::filesystem::path file(dir / "text.txt");
  CreateFlagFile(file, "testcount");
  string content = GetFlagFileContent(file);
  ASSERT_TRUE(!content.empty());
}