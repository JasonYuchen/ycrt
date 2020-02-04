cmake_minimum_required(VERSION 3.10)
project(ycrt)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

aux_source_directory(${PROJECT_SOURCE_DIR}/logdb SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/pb SOURCE)
#aux_source_directory(${PROJECT_SOURCE_DIR}/playground SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/raft SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/server SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/settings SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/statemachine SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/tests SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/transport SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/utils SOURCE)
aux_source_directory(${PROJECT_SOURCE_DIR}/ycrt SOURCE)
include_directories(.)

add_library(ycrt SHARED ${SOURCE})
target_link_libraries(ycrt
        PRIVATE
        libspdlog.a
        libprotobuf.so
        pthread)

add_library(ycrt_static STATIC ${SOURCE})
target_link_libraries(ycrt_static
        PRIVATE
        libspdlog.a
        libprotobuf.a
        pthread)
set_target_properties(ycrt_static PROPERTIES OUTPUT_NAME "ycrt")

if (UnitTest)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -DGTEST")
    include(GoogleTest)
    link_libraries(gtest gtest_main)
    add_executable(ycrt_test ${SOURCE})
    target_link_libraries(
            ycrt_test
            PRIVATE
            gtest
            gtest_main
            spdlog
            protobuf
            pthread)
    gtest_discover_tests(ycrt_test)
endif ()