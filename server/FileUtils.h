//
// Created by jason on 2020/2/5.
//

#ifndef YCRT_SERVER_FILEUTILS_H_
#define YCRT_SERVER_FILEUTILS_H_

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include "utils/Utils.h"

namespace ycrt
{

// SyncDir calls fsync on the specified directory.
Status SyncDir(const boost::filesystem::path &dir);

// MarkDirAsDeleted marks the specified directory as deleted.
Status MarkDirAsDeleted(const boost::filesystem::path &dir, string_view content);

// IsDirMarkedAsDeleted returns a boolean flag indicating whether the specified
// directory has been marked as deleted.
StatusWith<bool> IsDirMarkedAsDeleted(const boost::filesystem::path &dir);

Status CreateFlagFile(
  const boost::filesystem::path &filePath,
  string_view content);

StatusWith<std::string> GetFlagFileContent(const boost::filesystem::path &filePath);

} // namespace ycrt

#endif //YCRT_SERVER_FILEUTILS_H_
