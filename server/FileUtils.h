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

// File system errors are considered critical

// SyncDir calls fsync on the specified fd.
// throw if failed
void SyncFd(int fd);

// SyncDir calls fsync on the specified directory.
void SyncDir(const boost::filesystem::path &dir);

// MarkDirAsDeleted marks the specified directory as deleted.
void MarkDirAsDeleted(const boost::filesystem::path &dir, string_view content);

// IsDirMarkedAsDeleted returns a boolean flag indicating whether the specified
// directory has been marked as deleted.
bool IsDirMarkedAsDeleted(const boost::filesystem::path &dir);

// CreateFlagFile creates a flag file in the specific location. The flag file
// contains the marshaled data of the specified protobuf message.
void CreateFlagFile(
  const boost::filesystem::path &filePath,
  string_view content);

// RemoveFlagFile removes the specified flag file.
void RemoveFlagFile(const boost::filesystem::path &filePath);

// GetFlagFileContent gets the content of the flag file found in the specified
// location. The data of the flag file will be unmarshaled into the specified
// protobuf message.
std::string GetFlagFileContent(const boost::filesystem::path &filePath);

} // namespace ycrt

#endif //YCRT_SERVER_FILEUTILS_H_
