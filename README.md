# ycrt

**Re-implement** [dragonboat](github.com/lni/dragonboat) in modern C++.

## Dependencies

1. [boost.Asio](https://think-async.com/Asio/)
2. [boost.filesystem](https://www.boost.org/doc/libs/1_72_0/libs/filesystem/doc/index.htm)
3. [spdlog](https://github.com/gabime/spdlog)
4. protobuf (pending)
5. rocksdb (pending)
6. [concurrentqueue](https://github.com/cameron314/concurrentqueue)

## Progress

notation:

&lArr; currently working on

&times; not started yet

&radic; almost finished (need test, review logging, etc)

0. overall
    1. add docs in every module to explain the workflow &lArr;
    2. add tests (long term) &times;
    3. add benchmark (long term) &times;
    4. refactor transport when C++20 coroutine / C++23 network (long term) ?
1. logdb
    1. in memory reader (InMemory) &lArr;
    2. rocksdb reader (LogReader) &lArr;
    3. rocksdb adapter (LogDB) &times;
2. pb
    1. replace protobuf &times;
    2. add relevant methods &times;
3. raft
    1. core statemachine (Raft, Peer) &radic;
    2. prevote mechanism &times;
    3. quiesce manager &radic;
4. server
    1. file utils &radic;
    2. snapshot file environment (SnapshotEnv) &radic;
    3. double buffering message queue &lArr;
    4. context &times;
    5. rate limiter &times;
5. settings
    1. default settings (Soft, Hard) &radic;
    2. load settings from files &times;
6. statemachine
    1. state machine manager &lArr;
    2. state machine interface (Regular/Concurrent/OnDiskStateMachine) &radic;
    3. core state machine &times;
    4. snapshot manager (Snapshotter) &lArr;
    5. snapshot io (SnapshotWriter & SnapshotReader) &lArr;
    6. extra snapshot file collections (SnapshotFileSet) &radic;
    7. session (Session & SessionManager) &lArr;
    8. snapshot reader support compression &times;
7. tests &times;
8. transport
    1. transport interface (Transport) &lArr;
    2. remote node resolver (NodeResolver) &radic;
    3. normal message channel (SendChannel & RecvChannel) &radic;
    4. snapshot channel (SnapshotLane) &radic;
    5. snapshot chunk manager (SnapshotChunkManager) &radic;
    6. snapshot streaming &times;
    7. support timeout mechanism &radic;
    8. support Mutual TLS &times;
    9. support compression &times;
9. utils
    1. concurrent queue (cameron314::concurrentqueue)
    2. naive circuit breaker (CircuitBreaker) &radic;
    3. logging (gabime::spdlog, Logger)
    4. error reporting (Error) &radic;
    5. LRU cache (LRUCache) &radic;
    6. Read/Write buffer &times;
10. ycrt
    1. configuration (Config) &radic;
    2. raft node (Node) &lArr;
    3. ycrt interface (NodeHost) &times;
    4. core engine (ExecEngine) &times;

## Build

pending

## Test

pending

## License

ycrt is licensed under the Apache License Version 2.0.