cmake_minimum_required(VERSION 2.8.10)
project(dingo C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(LINK_TCMALLOC "Link tcmalloc if possible" OFF)
option(BUILD_UNIT_TESTS "Build unit test" ON)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(THIRD_PARTY_PATH ${CMAKE_CURRENT_BINARY_DIR}/third-party)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

#include(FindThreads)
find_package(OpenSSL REQUIRED)
include(zlib)
include(gflags)
include(glog)
include(gtest)
include(snappy)
include(leveldb)
include(protobuf)
include(rocksdb)
include(brpc)
include(braft)
include(yaml-cpp)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -std=c++17")

# LIST ALL THE PROTO FILES AND COMPILE IT TO .H AND .CPP
file(GLOB PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/*.proto)
message("protoc: ${PROTOBUF_PROTOC_EXECUTABLE}, proto inc: ${PROTOBUF_INCLUDE_DIRS}, lib: ${PROTOBUF_LIBRARIES}, ${PROTOBUF_PROTOC_LIBRARY}, protos: ${PROTO_FILES}")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
foreach (PROTO ${PROTO_FILES})
    message(proto : ${PROTO})
    get_filename_component(PROTO_WE ${PROTO} NAME)
    string(REPLACE ".proto" "" PROTO_WE ${PROTO_WE})
    list(APPEND PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.h")
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.cc")
    add_custom_command(
            OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.h ${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.cc
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/proto
            --proto_path=${PROTOBUF_INCLUDE_DIR}
            --proto_path=${CMAKE_SOURCE_DIR}/proto ${PROTO}
            DEPENDS protobuf
    )
endforeach ()
add_library(PROTO_OBJS OBJECT ${PROTO_SRCS} ${PROTO_HEADER})
message("Debug Message protoc: ${PROTOBUF_PROTOC_EXECUTABLE}, proto srcs : ${PROTO_SRCS}")

# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${BRAFT_INCLUDE_DIR})
include_directories(${BRPC_INCLUDE_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIR})
include_directories(${YAMLCPP_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src)

message(STATUS "===GTEST_LIBRARIES: ${GTEST_LIBRARIES}")

set(DYNAMIC_LIB
    ${GFLAGS_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    # ${GPERFTOOLS_LIBRARIES}
    ${LEVELDB_LIBRARIES}
    ${BRAFT_LIBRARIES}
    ${BRPC_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    ${SNAPPY_LIBRARIES}
    ${YAMLCPP_LIBRARIES}
    rt
    ssl
    crypto
    glog
    dl
    z
    )

set(DEPEND_LIBS
    gflags
    protobuf
    leveldb
    braft
    brpc
    rocksdb
    snappy
    glog
    yamlcpp
    )

# source file
file(GLOB CONFIG_SRCS ${PROJECT_SOURCE_DIR}/src/config/*.cc)
file(GLOB STORE_SRCS ${PROJECT_SOURCE_DIR}/src/store/*.cc)

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

# set(STORE_SRCS src/store/main.cc src/store/store_service.cc)
add_executable(dingo_store ${STORE_SRCS} ${CONFIG_SRCS} ${PROTO_SRCS} ${PROTO_HEADER})
add_executable(dingo_store_client src/client/store_client.cc ${PROTO_SRCS} ${PROTO_HEADER})
add_dependencies(dingo_store ${DEPEND_LIBS})
add_dependencies(dingo_store_client ${DEPEND_LIBS})
target_link_libraries(dingo_store
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")
target_link_libraries(dingo_store_client
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")


if(BUILD_UNIT_TESTS)
    add_subdirectory(test)
endif()