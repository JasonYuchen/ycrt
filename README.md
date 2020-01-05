# ycrt

Yet another Raft-based consensus library in modern C++.

Deeply inspired by dragonboat. (could be considered as a C++ version of dragonboat)

## dependencies

1. boost.Asio
2. spdlog
3. protobuf (pending)
4. rocksdb (pending)

## current

1. replace protobuf with hand-written serialization (pb/raft.pb.h)
2. core raft protocal (raft/Raft.h)
3. raft storage (raft/LogEntry.h)
4. transport module (transport/Transport.h)