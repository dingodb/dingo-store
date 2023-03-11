# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)
project(dingo C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(LINK_TCMALLOC "Link tcmalloc if possible" OFF)
option(BUILD_UNIT_TESTS "Build unit test" ON)
option(DINGO_BUILD_STATIC "Link libraries statically to generate the DingoDB binary" OFF)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-std=c++17" COMPILER_SUPPORTS_CXX17)
if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support. Please use a different C++ compiler.")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE "Debug")

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(THIRD_PARTY_PATH ${CMAKE_CURRENT_BINARY_DIR}/third-party)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

find_package(Threads REQUIRED)

include(openssl)
include(zlib)
include(gflags)
include(glog)
include(gtest)
include(snappy)
include(lz4)
include(zstd)
include(leveldb)
include(protobuf)
include(rocksdb)
include(brpc)
include(braft)
include(yaml-cpp)

message("protoc: ${PROTOBUF_PROTOC_EXECUTABLE}, proto inc: ${PROTOBUF_INCLUDE_DIRS}, lib: ${PROTOBUF_LIBRARIES}, ${PROTOBUF_PROTOC_LIBRARY}, protos: ${PROTO_FILES}")
SET(MESSAGE_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
if(EXISTS "${CMAKE_CURRENT_BINARY_DIR}/proto" AND IS_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/proto")
        SET(PROTO_META_BASE_DIR ${MESSAGE_DIR})
else()
        file(MAKE_DIRECTORY ${MESSAGE_DIR})
        SET(PROTO_META_BASE_DIR ${MESSAGE_DIR})
endif()

LIST(APPEND PROTO_FLAGS -I${CMAKE_SOURCE_DIR}/proto)
file(GLOB_RECURSE MSG_PROTOS ${CMAKE_SOURCE_DIR}/proto/*.proto)
set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(msg ${MSG_PROTOS})
        get_filename_component(FIL_WE ${msg} NAME_WE)

        list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.cc")
        list(APPEND PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.h")

        add_custom_command(
          OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.cc"
                 "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.h"
          COMMAND  ${PROTOBUF_PROTOC_EXECUTABLE}
          ARGS --cpp_out  ${PROTO_META_BASE_DIR}
            -I ${CMAKE_SOURCE_DIR}/proto
            ${msg}
          DEPENDS protobuf ${msg}
          COMMENT "Running C++ protocol buffer compiler on ${msg}"
          VERBATIM
        )
endforeach()
set_source_files_properties(${PROTO_SRCS} ${PROTO_HDRS} PROPERTIES GENERATED TRUE)

add_library(PROTO_OBJS OBJECT ${PROTO_SRCS})
message("Debug Message protoc: ${PROTOBUF_PROTOC_EXECUTABLE}, proto srcs : ${PROTO_SRCS}")

add_custom_target(build_proto ALL
                DEPENDS ${PROTO_SRCS} ${PROTO_HDRS}
                COMMENT "generate message target"
                VERBATIM
                )

# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${BRAFT_INCLUDE_DIR})
include_directories(${BRPC_INCLUDE_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${GTEST_INCLUDE_DIR})
include_directories(${GFLAGS_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIR})
include_directories(${YAMLCPP_INCLUDE_DIR})
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src)

set(DYNAMIC_LIB
    ${GFLAGS_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${LEVELDB_LIBRARIES}
    ${BRAFT_LIBRARIES}
    ${BRPC_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    ${SNAPPY_LIBRARIES}
    ${LZ4_LIBRARIES}
    ${ZSTD_LIBRARIES}
    ${YAMLCPP_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${GTEST_MAIN_LIBRARIES}
    ${GTEST_LIBRARIES}
    # rt
    dl
    Threads::Threads
    )

set(DEPEND_LIBS
    openssl
    zlib
    gflags
    protobuf
    leveldb
    braft
    brpc
    rocksdb
    snappy
    lz4
    zstd
    glog
    yamlcpp
    gtest
    )

# source file
file(GLOB COMMON_SRCS ${PROJECT_SOURCE_DIR}/src/common/*.cc)
file(GLOB CONFIG_SRCS ${PROJECT_SOURCE_DIR}/src/config/*.cc)
file(GLOB RAFT_SRCS ${PROJECT_SOURCE_DIR}/src/raft/*.cc)
file(GLOB ENGINE_SRCS ${PROJECT_SOURCE_DIR}/src/engine/*.cc)
file(GLOB CRONTAB_SRCS ${PROJECT_SOURCE_DIR}/src/crontab/*.cc)
file(GLOB META_SRCS ${PROJECT_SOURCE_DIR}/src/meta/*.cc)
file(GLOB COORDINATOR_SRCS ${PROJECT_SOURCE_DIR}/src/coordinator/*.cc)
file(GLOB STORE_SRCS ${PROJECT_SOURCE_DIR}/src/store/*.cc)
file(GLOB SERVER_SRCS ${PROJECT_SOURCE_DIR}/src/server/*.cc)

list(REMOVE_ITEM SERVER_SRCS "${PROJECT_SOURCE_DIR}/src/server/main.cc")

# object file
add_library(DINGODB_OBJS
            OBJECT
            ${COMMON_SRCS}
            ${CONFIG_SRCS}
            ${RAFT_SRCS}
            ${ENGINE_SRCS}
            ${CRONTAB_SRCS}
            ${META_SRCS}
            ${COORDINATOR_SRCS}
            ${STORE_SRCS}
            ${SERVER_SRCS})

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

add_executable(dingodb_server src/server/main.cc $<TARGET_OBJECTS:DINGODB_OBJS> $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(dingodb_client_store src/client/store_client.cc $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(dingodb_client_coordinator src/client/coordinator_client.cc $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(dingodb_client_meta src/client/meta_client.cc $<TARGET_OBJECTS:PROTO_OBJS>)

add_dependencies(DINGODB_OBJS ${DEPEND_LIBS})
add_dependencies(dingodb_server ${DEPEND_LIBS})
add_dependencies(dingodb_client_store ${DEPEND_LIBS})
add_dependencies(dingodb_client_coordinator ${DEPEND_LIBS})
add_dependencies(dingodb_client_meta ${DEPEND_LIBS})

if(DINGO_BUILD_STATIC)
    message(STATUS "Build DingoDB with static libraries linking")
    set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
endif()

target_link_libraries(dingodb_server
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")
target_link_libraries(dingodb_client_store
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")
target_link_libraries(dingodb_client_coordinator
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")
target_link_libraries(dingodb_client_meta
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")

if(BUILD_UNIT_TESTS)
    add_subdirectory(test)
endif()
