# ycrt

**Re-implement** [dragonboat](github.com/lni/dragonboat) in modern C++.

## Dependencies

1. boost.Asio (Transport)
2. boost.filesystem (Transport Snapshot)
3. spdlog (Logger)
4. protobuf (pending)
5. rocksdb (pending)
6. [concurrentqueue](https://github.com/cameron314/concurrentqueue)

## Progress

notation:

&lArr; currently working on

&times; not started yet

&radic; almost finished (need test, review logging, etc)

1. logdb
    1. in memory reader (InMemory) &lArr;
    2. rocksdb reader (LogReader) &lArr;
    3. rocksdb adapter &times;
2. pb
    1. replace protobuf &times;
    2. add relevant methods &times;
3. raft
    1. core statemachine &radic;
    2. prevote mechanism &times;
4. server
    1. file utils &lArr;
    2. 
5. settings
    1. default settings &radic;
    2. load settings from files &times;
6. statemachine &times;
7. tests &times;
8. transport
    1. transport interface (Transport) &lArr;
    2. remote node resolver (Nodes) &radic;
    3. normal message channel (SendChannel & RecvChannel) &radic;
    4. snapshot channel &lArr;
    5. snapshot chunk manager &lArr;
9. utils &lArr;
10. ycrt
    1. configuration &radic;
    2. raft node (Node) &times;
    3. ycrt interface (NodeHost) &times;
    4. core engine (ExecEngine) &times;
    5. 

## Build

pending

## Test

pending

## License

Apache 2.0