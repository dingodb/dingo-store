# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

# Note: if BRPC_ENABLE_CPU_PROFILER is ON, there may be child process hang because of the sample process of tcmalloc
# will cause dead lock after fork, so do not enable DBRPC_ENABLE_CPU_PROFILER in production enviroment. For develop
# without MKL: cmake .. -DCMAKE_BUILD_TYPE=Debug -DTHIRD_PARTY_BUILD_TYPE=Debug -DBRPC_ENABLE_CPU_PROFILER=ON
# -DLINK_TCMALLOC=ON For develop with MKL: cmake .. -DCMAKE_BUILD_TYPE=Debug -DTHIRD_PARTY_BUILD_TYPE=Debug
# -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=ON -DLINK_TCMALLOC=ON For RelWithDbgInfo: cmake ..
# -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTHIRD_PARTY_BUILD_TYPE=RelWithDebInfo -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=OFF
# -DLINK_TCMALLOC=ON For RelWithDbgInfo: cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
# -DTHIRD_PARTY_BUILD_TYPE=RelWithDebInfo -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=OFF -DLINK_TCMALLOC=ON For Release:
# cmake .. -DCMAKE_BUILD_TYPE=Release -DTHIRD_PARTY_BUILD_TYPE=Release -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=OFF
# -DLINK_TCMALLOC=ON Build all binaries include unit test: make Build only product binaries: make dingodb_server
# dingodb_client

# if compile_commands.json is needed, please enable CMAKE_EXPORT_COMPILE_COMMANDS, of use `bear --append -- make` to do
# make, it's more recommended to use bear.

cmake_minimum_required(VERSION 3.23.1 FATAL_ERROR)
project(dingo-store C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(LINK_TCMALLOC "Link tcmalloc if possible" ON)
option(BUILD_UNIT_TESTS "Build unit test" OFF)
option(ENABLE_COVERAGE "Enable unit test code coverage" OFF)
option(DINGO_BUILD_STATIC "Link libraries statically to generate the dingodb binary" ON)
option(ENABLE_FAILPOINT "Enable failpoint" OFF)
option(WITH_DISKANN "Build with diskann index" OFF)
option(WITH_MKL "Build with intel mkl" OFF)
option(BOOST_SEARCH_PATH "")
option(BUILD_GOOGLE_SANITIZE "Enable google sanitize" OFF)
option(BRPC_ENABLE_CPU_PROFILER "Enable brpc cpu profiler" OFF)
option(XDPROCKS_PATH "Enable xdprocks raw engine")
option(VECTORIZATION_INSTRUCTION_SET "vectorization instruction set")
option(WITH_LIBURING "Build with liburing" ON)

message(STATUS CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
message(STATUS THIRD_PARTY_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE})

set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -DUSE_BTHREAD_MUTEX)

set(CMAKE_C_FLAGS_DEBUG "$ENV{CFLAGS} -O0  -g3 -ggdb")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0  -g3 -ggdb")

message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")
message(STATUS "SYSTEM: ${CMAKE_HOST_SYSTEM_NAME} ${CMAKE_HOST_SYSTEM_PROCESSOR}")
message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

# xdprocks
if(XDPROCKS_PATH)
  message(STATUS "Enable xdprocks, path: ${XDPROCKS_PATH}")
  if(NOT EXISTS ${XDPROCKS_PATH})
    message(FATAL_ERROR "xdprocks path(${XDPROCKS_PATH}) is not exist.")
  endif()
  if(NOT EXISTS ${XDPROCKS_PATH}/lib/libxdprocks.a)
    message(FATAL_ERROR "libxdprocks.a is not exist.")
  endif()
  if(NOT EXISTS ${XDPROCKS_PATH}/lib/libstorelib.a)
    message(FATAL_ERROR "libstorelib.a is not exist.")
  endif()
  if(NOT EXISTS ${XDPROCKS_PATH}/lib/libzlog.a)
    message(FATAL_ERROR "libzlog.a is not exist.")
  endif()
  set(ENABLE_XDPROCKS ON)
  add_definitions(-DENABLE_XDPROCKS=ON)

  execute_process(COMMAND sh ${CMAKE_CURRENT_SOURCE_DIR}/scripts/prepare_xdprocks.sh --xdprocks_dir ${XDPROCKS_PATH})
else()
  message(STATUS "Disable xdprocks")
  set(ENABLE_XDPROCKS OFF)
endif()

# third-party install path
if(NOT DINGO_EUREKA_INSTALL_PATH)
  if(DEFINED ENV{DINGO_EUREKA_INSTALL_PATH})
    set(DINGO_EUREKA_INSTALL_PATH $ENV{DINGO_EUREKA_INSTALL_PATH})
  else()
    set(DINGO_EUREKA_INSTALL_PATH "$ENV{HOME}/.local/dingo-eureka")
  endif()
endif()
message("DINGO_EUREKA_INSTALL_PATH:${DINGO_EUREKA_INSTALL_PATH}")

include_directories(${DINGO_EUREKA_INSTALL_PATH}/include)

set(CMAKE_PREFIX_PATH
  ${DINGO_EUREKA_INSTALL_PATH}
  ${CMAKE_PREFIX_PATH}
  )
message("CMAKE_PREFIX_PATH:${CMAKE_PREFIX_PATH}")

set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
message("CMAKE_FIND_LIBRARY_SUFFIXES: ${CMAKE_FIND_LIBRARY_SUFFIXES}")

if(BRPC_ENABLE_CPU_PROFILER)
  message(STATUS "Enable BRPC_ENABLE_CPU_PROFILER")
  add_definitions(-DBRPC_ENABLE_CPU_PROFILER=ON)
  set(LINK_TCMALLOC ON)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(BUILD_GOOGLE_SANITIZE)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address -fno-omit-frame-pointer)
  add_definitions(-DUSE_SANITIZE=ON)
  message(STATUS "enable google sanitize")
endif()

if(ENABLE_COVERAGE)
  message(STATUS "Enable coverage")
  add_compile_options(-fprofile-arcs -ftest-coverage)
endif()

if(${VECTORIZATION_INSTRUCTION_SET} STREQUAL "avx2")
  message(STATUS "Use avx2 instruction set")
  add_compile_options(-mavx2)
elseif(${VECTORIZATION_INSTRUCTION_SET} STREQUAL "avx512")
  message(STATUS "Use avx512 instruction set")
  add_compile_options(-mavx512f)
endif()

include(CheckCXXCompilerFlag)
if(NOT CMAKE_CXX_STANDARD)
  message(STATUS "CMAKE_CXX_STANDARD is not set, will set to 17")
  set(CMAKE_CXX_STANDARD 17)
else()
  set(ALLOWED_CXX_STANDARDS 17 20 23)
  set_property(CACHE CMAKE_CXX_STANDARD PROPERTY STRINGS ${ALLOWED_CXX_STANDARDS})
  list(FIND ALLOWED_CXX_STANDARDS ${CMAKE_CXX_STANDARD} POSITION)
  if(POSITION LESS 0)
    message(FATAL_ERROR "Invalid CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}. "
                        "Must be one of: ${ALLOWED_CXX_STANDARDS}")
  endif()
endif()

if(CMAKE_CXX_STANDARD EQUAL 23)
  check_cxx_compiler_flag("-std=c++23" COMPILER_SUPPORTS_CXX23)
  if(COMPILER_SUPPORTS_CXX23)
    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
  else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++23 support. Please use a different C++ compiler.")
  endif()
elseif(CMAKE_CXX_STANDARD EQUAL 20)
  check_cxx_compiler_flag("-std=c++20" COMPILER_SUPPORTS_CXX20)
  if(COMPILER_SUPPORTS_CXX20)
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
  else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++20 support. Please use a different C++ compiler.")
  endif()
else()
  check_cxx_compiler_flag("-std=c++17" COMPILER_SUPPORTS_CXX17)
  if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
  else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support. Please use a different C++ compiler.")
  endif()
endif()

if(THIRD_PARTY_BUILD_TYPE MATCHES "Debug")
  set(CMAKE_STATIC_LIBRARY_SUFFIX "d.a")
endif()
set(EXTERNAL_PROJECT_LOG_ARGS
    LOG_DOWNLOAD
    0
    LOG_UPDATE
    1
    LOG_CONFIGURE
    0
    LOG_BUILD
    0
    LOG_TEST
    1
    LOG_INSTALL
    0)

if(CMAKE_CXX_STANDARD EQUAL 23)
  set(CMAKE_CXX_STANDARD 23)
elseif(CMAKE_CXX_STANDARD EQUAL 20)
  set(CMAKE_CXX_STANDARD 20)
else()
  set(CMAKE_CXX_STANDARD 17)
endif()
# set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")

set(THIRD_PARTY_PATH ${CMAKE_CURRENT_BINARY_DIR}/third-party)

set(CMAKE_VERBOSE_MAKEFILE ON)

execute_process(COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/contrib/.clang-tidy ${CMAKE_CURRENT_BINARY_DIR}/)

find_package(Threads REQUIRED)

find_package(OpenMP REQUIRED)

execute_process(
  COMMAND g++ --print-file-name libgomp.a
  RESULT_VARIABLE CMD_RESULT
  OUTPUT_VARIABLE FILE_NAME
  OUTPUT_STRIP_TRAILING_WHITESPACE)

# Check the return code and output of the command
if(CMD_RESULT EQUAL 0 AND EXISTS ${FILE_NAME})
  message(STATUS "Valid libgomp: ${FILE_NAME}")
  set(OPENMP_LIBRARY ${FILE_NAME})
elseif(DINGO_BUILD_STATIC)
  message(FATAL_ERROR "static libgomp is not found, file_name: ${FILE_NAME}")
else()
  set(OPENMP_LIBRARY ${OpenMP_gomp_LIBRARY})
endif()

message(STATUS "OpenMP library: ${OPENMP_LIBRARY}")

# find third-party
set(gflags_DIR ${DINGO_EUREKA_INSTALL_PATH}/lib/cmake/gflags)
find_package(gflags REQUIRED)
message("Using gflags ${gflags_VERSION}, include_dir:${gflags_INCLUDE_DIR}")

# openssl
if(NOT OPENSSL_ROOT_DIR) 
  set(OPENSSL_ROOT_DIR ${DINGO_EUREKA_INSTALL_PATH})
endif()
find_package(OpenSSL REQUIRED)
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}, include dir: ${OPENSSL_INCLUDE_DIR}")

find_package(Snappy)
message("Using Snappy ${Snappy_VERSION}")

find_package(fmt REQUIRED)
message("Using fmt ${fmt_VERSION}")

set(ZLIB_USE_STATIC_LIBS "ON")
find_package(ZLIB REQUIRED)
message("Using zlib ${ZLIB_VERSION}, include_dir:${ZLIB_INCLUDE_DIR}")

find_package (glog REQUIRED)
# used for glog 0.7.0
add_compile_definitions(GLOG_USE_GLOG_EXPORT)
message("Using glog ${glog_VERSION}")

find_package(GTest CONFIG REQUIRED)
message("Using GTest ${GTest_VERSION}")

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message("Using protobuf ${Protobuf_VERSION}" )

find_program(PROTOC protoc REQUIRED)
message("Using protoc ${PROTOC}")

find_package(nlohmann_json REQUIRED)
message("Using nlohmann_json ${nlohmann_json_VERSION}")

find_package(RapidJSON REQUIRED)
message("Using RapidJSON ${RapidJSON_VERSION}, include_dir:${RapidJSON_INCLUDE_DIR}")

find_package(leveldb REQUIRED)
find_package(brpc REQUIRED)
find_package(braft REQUIRED)
find_package(uring REQUIRED)
find_package(unwind REQUIRED)
find_package(gperftools REQUIRED)
find_package(backtrace REQUIRED)

# provoided by rocksdb cmake
find_package(RocksDB REQUIRED)

include(yaml-cpp)
include(ftxui)
include(hnswlib)
include(bdb)
include(tantivy-search)

set(MESSAGE_SOURCE_DIR ${CMAKE_SOURCE_DIR}/dingo-store-proto/proto)
set(MESSAGE_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)

if(EXISTS ${MESSAGE_DIR} AND IS_DIRECTORY ${MESSAGE_DIR})
  set(PROTO_META_BASE_DIR ${MESSAGE_DIR})
else()
  file(MAKE_DIRECTORY ${MESSAGE_DIR})
  set(PROTO_META_BASE_DIR ${MESSAGE_DIR})
endif()

list(APPEND PROTO_FLAGS -I${MESSAGE_SOURCE_DIR})
file(GLOB_RECURSE MSG_PROTOS ${MESSAGE_SOURCE_DIR}/*.proto)
set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(msg ${MSG_PROTOS})
  get_filename_component(FIL_WE ${msg} NAME_WE)

  message(STATUS "proto file: ${msg}")

  list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.cc")
  list(APPEND PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.h")

  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.cc" "${CMAKE_CURRENT_BINARY_DIR}/proto/${FIL_WE}.pb.h"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ARGS --cpp_out ${PROTO_META_BASE_DIR} -I ${MESSAGE_SOURCE_DIR} ${msg}
    DEPENDS ${msg}
    COMMENT "Running C++ protocol buffer compiler on ${msg}"
    VERBATIM)
endforeach()
set_source_files_properties(${PROTO_SRCS} ${PROTO_HDRS} PROPERTIES GENERATED TRUE)

add_library(PROTO_OBJS OBJECT ${PROTO_SRCS})
set_target_properties(PROTO_OBJS PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_custom_target(
  build_proto ALL
  DEPENDS ${PROTO_SRCS} ${PROTO_HDRS}
  COMMENT "generate message target"
  VERBATIM)

# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/serial/src/)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/libexpr/src/)
include_directories(${BDB_INCLUDE_DIR})
if(ENABLE_XDPROCKS)
  include_directories(${XDPROCKS_PATH}/include)
endif()
include_directories(${YAMLCPP_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${TANTIVY_SEARCH_INCLUDE_DIR})
include_directories(${FTXUI_INCLUDE_DIR})

# CAUTION: the order of the libraries is important, please do not change it.
set(DYNAMIC_LIB
    braft::braft
    brpc::brpc
    protobuf::libprotobuf
    gflags
    leveldb::leveldb
    RocksDB::rocksdb
    Snappy::snappy
    fmt::fmt
    glog::glog
    GTest::gtest
    GTest::gmock
    OpenSSL::SSL
    unwind::unwind
    unwind::unwind-generic
    backtrace::backtrace
    ZLIB::ZLIB
    ${YAMLCPP_LIBRARIES}
    ${BDB_LIBRARIES}
    ${TANTIVY_SEARCH_LIBRARIES}
    ${FTXUI_COMPONENT_LIBRARY}
    ${FTXUI_DOM_LIBRARY}
    ${FTXUI_SCREEN_LIBRARY})

if(ENABLE_XDPROCKS)
  set(DYNAMIC_LIB ${DYNAMIC_LIB} "${XDPROCKS_PATH}/lib/libxdprocks.a" "${XDPROCKS_PATH}/lib/libstorelib.a"
                  "${XDPROCKS_PATH}/lib/libzlog.a" "-l:libpci.so.3" "-l:libudev.so.1")
endif()

set(DYNAMIC_LIB ${DYNAMIC_LIB} dl Threads::Threads)

if(ENABLE_COVERAGE)
  set(DYNAMIC_LIB ${DYNAMIC_LIB} gcov)
endif()

if(WITH_LIBURING)
  set(DYNAMIC_LIB ${DYNAMIC_LIB} uring::uring)
endif()

set(DEPEND_LIBS
    yamlcpp
    faiss
    bdb
    tantivy-search
    ftxui)

if(WITH_MKL)
  if(DEFINED ENV{MKLROOT})
    message(STATUS "MKLROOT is: $ENV{MKLROOT}")
    set(MKLROOT $ENV{MKLROOT})
  else()
    message(FATAL_ERROR "NOT DEFINED MKLROOT VARIABLES")
  endif()

  set(MKL_LIBRARIES)

  set(INT_LIB "libmkl_intel_lp64.a")
  set(SEQ_LIB "libmkl_sequential.a")
  set(COR_LIB "libmkl_core.a")

  find_path(
    MKL_ROOT include/mkl.h
    PATHS ${MKLROOT}
    DOC "Folder contains MKL")
  find_path(
    MKL_INCLUDE_DIR
    NAMES mkl.h
    HINTS ${MKL_ROOT}/include)

  find_library(
    MKL_INTERFACE_LIBRARY
    NAMES ${INT_LIB}
    PATHS ${MKL_ROOT}/lib/intel64)

  find_library(
    MKL_SEQUENTIAL_LAYER_LIBRARY
    NAMES ${SEQ_LIB}
    PATHS ${MKL_ROOT}/lib/intel64)

  find_library(
    MKL_CORE_LIBRARY
    NAMES ${COR_LIB}
    PATHS ${MKL_ROOT}/lib/intel64)

  set(MKL_LIBRARIES ${MKL_SEQUENTIAL_LAYER_LIBRARY} ${MKL_INTERFACE_LIBRARY} ${MKL_CORE_LIBRARY})
  set(BLAS_LIBRARIES ${MKL_LIBRARIES})

  message(STATUS "MKL_LIBRARIES=${MKL_LIBRARIES}")

  add_definitions(-DUSE_MKL=ON)

  include_directories(${MKL_INCLUDE_DIR})
  include(faiss-mkl)

else()
  # find_package(MKL QUIET) if(MKL_FOUND) message(FATAL_ERROR "The MKL is found, cannot build faiss with openblas,
  # please disable MKL.") endif()

  message(STATUS "Enable USE_OPENBLAS")
  include(openblas)
  include_directories(${OPENBLAS_INCLUDE_DIR})

  set(DEPEND_LIBS ${DEPEND_LIBS} openblas)
  set(BLAS_LIBRARIES ${OPENBLAS_LIBRARIES})

  add_definitions(-DUSE_OPENBLAS=ON)

  include(faiss-openblas)
endif()

include_directories(${FAISS_INCLUDE_DIR})
set(VECTOR_LIB ${FAISS_LIBRARIES} ${OPENMP_LIBRARY})

if(WITH_DISKANN)
  add_definitions(-DENABLE_DISKANN_MODULE=ON)
  if(NOT WITH_MKL)
    message(FATAL_ERROR "The WITH_MKL is not ON, please install enable WITH_MKL to build diskann.")
  endif()

  # if(NOT MKL_FOUND) message( FATAL_ERROR "The MKL is not found, please install intel mkl to build diskann.") endif()

  find_package(MKL CONFIG REQUIRED)

  if(NOT BOOST_SEARCH_PATH)
    find_package(Boost REQUIRED COMPONENTS program_options)
  else()
    message(STATUS "BOOST_SEARCH_PATH=${BOOST_SEARCH_PATH}, use user-defined boost version")
  endif()

  include(diskann)
  include_directories(${DISKANN_INCLUDE_DIR})
  set(DEPEND_LIBS ${DEPEND_LIBS} diskann)
  set(VECTOR_LIB ${VECTOR_LIB} ${DISKANN_LIBRARIES} aio)
endif()

if(LINK_TCMALLOC)
  add_definitions(-DLINK_TCMALLOC=ON)

  message(STATUS "Build DingoDB with tcmalloc")

  if(BRPC_ENABLE_CPU_PROFILER)
    set(DYNAMIC_LIB ${DYNAMIC_LIB} gperftools::gperftools)
  else()
    set(DYNAMIC_LIB ${DYNAMIC_LIB} gperftools::gperftools_minimal)
  endif()
endif()


# source file
file(GLOB_RECURSE DINGO_SERIAL_SOURCES ${PROJECT_SOURCE_DIR}/src/serial/src/*.cc)
file(GLOB_RECURSE DINGO_SERIAL_SCHEMA_SOURCES ${PROJECT_SOURCE_DIR}/src/serial/src/schema*.cc)

set(SERIAL_SRCS ${DINGO_SERIAL_SOURCES} ${DINGO_SERIAL_SCHEMA_SOURCES})

file(GLOB COMMON_SRCS ${PROJECT_SOURCE_DIR}/src/common/*.cc)
file(GLOB CONFIG_SRCS ${PROJECT_SOURCE_DIR}/src/config/*.cc)
file(GLOB LOG_SRCS ${PROJECT_SOURCE_DIR}/src/log/*.cc)
file(GLOB REPORT_SRCS ${PROJECT_SOURCE_DIR}/src/report/*.cc)
file(GLOB VECTOR_SRCS ${PROJECT_SOURCE_DIR}/src/vector/*.cc)
file(GLOB DOCUMENT_SRCS ${PROJECT_SOURCE_DIR}/src/document/*.cc)
file(GLOB SPLIT_SRCS ${PROJECT_SOURCE_DIR}/src/split/*.cc)
file(GLOB RAFT_SRCS ${PROJECT_SOURCE_DIR}/src/raft/*.cc)
file(GLOB ENGINE_SRCS ${PROJECT_SOURCE_DIR}/src/engine/*.cc)
file(GLOB CRONTAB_SRCS ${PROJECT_SOURCE_DIR}/src/crontab/*.cc)
file(GLOB HANDLER_SRCS ${PROJECT_SOURCE_DIR}/src/handler/*.cc)
file(GLOB EVENT_SRCS ${PROJECT_SOURCE_DIR}/src/event/*.cc)
file(GLOB META_SRCS ${PROJECT_SOURCE_DIR}/src/meta/*.cc)
file(GLOB MVCC_SRCS ${PROJECT_SOURCE_DIR}/src/mvcc/*.cc)
file(GLOB COORDINATOR_SRCS ${PROJECT_SOURCE_DIR}/src/coordinator/*.cc)
file(GLOB STORE_SRCS ${PROJECT_SOURCE_DIR}/src/store/*.cc)
file(GLOB SERVER_SRCS ${PROJECT_SOURCE_DIR}/src/server/*.cc)
file(GLOB SCAN_SRCS ${PROJECT_SOURCE_DIR}/src/scan/*.cc)
file(GLOB METRICS_SRCS ${PROJECT_SOURCE_DIR}/src/metrics/*.cc)
file(GLOB VERSION_SRCS ${PROJECT_SOURCE_DIR}/src/common/version.cc)
file(GLOB_RECURSE LIBEXPR_SRCS ${PROJECT_SOURCE_DIR}/src/libexpr/src/*.cc)
file(GLOB COPROCESSOR_SRCS ${PROJECT_SOURCE_DIR}/src/coprocessor/*.cc)
file(GLOB CLIENT_SRCS ${PROJECT_SOURCE_DIR}/src/client/*.cc)
file(GLOB CLIENT_V2_SRCS ${PROJECT_SOURCE_DIR}/src/client_v2/*.cc)
file(GLOB MERGE_SRCS ${PROJECT_SOURCE_DIR}/src/merge/*.cc)
if(WITH_DISKANN)
  file(GLOB DISKANN_SRCS ${PROJECT_SOURCE_DIR}/src/diskann/*.cc)
endif()
file(GLOB BR_SRCS ${PROJECT_SOURCE_DIR}/src/br/*.cc)

if(NOT ENABLE_XDPROCKS)
  list(REMOVE_ITEM ENGINE_SRCS "${PROJECT_SOURCE_DIR}/src/engine/xdprocks_raw_engine.cc")
endif()

list(REMOVE_ITEM SERVER_SRCS "${PROJECT_SOURCE_DIR}/src/server/main.cc")
if(NOT WITH_DISKANN)
  list(REMOVE_ITEM SERVER_SRCS "${PROJECT_SOURCE_DIR}/src/server/diskann_service.cc")
endif()

list(REMOVE_ITEM BR_SRCS "${PROJECT_SOURCE_DIR}/src/br/main.cc")

include(CheckSymbolExists)

macro(detect_target_arch)
  check_symbol_exists(__aarch64__ "" __AARCH64)
  check_symbol_exists(__x86_64__ "" __X86_64)

  if(NOT __AARCH64 AND NOT __X86_64)
    message(FATAL "dingodb only support amd64 and arm64.")
  endif()
endmacro()

detect_target_arch()

if(__X86_64)
  set(SIMD_UTILS_SRC ${PROJECT_SOURCE_DIR}/src/simd/distances_ref.cc ${PROJECT_SOURCE_DIR}/src/simd/hook.cc)
  set(SIMD_UTILS_SSE_SRC ${PROJECT_SOURCE_DIR}/src/simd/distances_sse.cc)
  set(SIMD_UTILS_AVX_SRC ${PROJECT_SOURCE_DIR}/src/simd/distances_avx.cc)
  set(SIMD_UTILS_AVX512_SRC ${PROJECT_SOURCE_DIR}/src/simd/distances_avx512.cc)

  add_library(simd_utils_sse OBJECT ${SIMD_UTILS_SSE_SRC})
  add_library(simd_utils_avx OBJECT ${SIMD_UTILS_AVX_SRC})
  add_library(simd_utils_avx512 OBJECT ${SIMD_UTILS_AVX512_SRC})

  target_compile_options(simd_utils_sse PRIVATE -msse4.2)
  target_compile_options(simd_utils_avx PRIVATE -mf16c -mavx2)
  target_compile_options(simd_utils_avx512 PRIVATE -mf16c -mavx512f -mavx512dq -mavx512bw)

  add_library(simd_utils STATIC ${SIMD_UTILS_SRC} $<TARGET_OBJECTS:simd_utils_sse> $<TARGET_OBJECTS:simd_utils_avx>
                                $<TARGET_OBJECTS:simd_utils_avx512>)
  # target_link_libraries(simd_utils PUBLIC glog::glog)
endif()

if(__AARCH64)
  set(SIMD_UTILS_SRC ${PROJECT_SOURCE_DIR}/src/simd/hook.cc ${PROJECT_SOURCE_DIR}/src/simd/distances_ref.cc)
  add_library(simd_utils STATIC ${SIMD_UTILS_SRC})
  # target_link_libraries(simd_utils PUBLIC glog::glog)
endif()

if(CMAKE_BUILD_TYPE MATCHES "Debug")
  set(SIMD_LIBRARIES
      "${CMAKE_BINARY_DIR}/libsimd_utilsd.a"
      CACHE FILEPATH "simd library." FORCE)
else()
  set(SIMD_LIBRARIES
      "${CMAKE_BINARY_DIR}/libsimd_utils.a"
      CACHE FILEPATH "simd library." FORCE)
endif()

set(DEPEND_LIBS ${DEPEND_LIBS} simd_utils)

set(DYNAMIC_LIB ${DYNAMIC_LIB} ${SIMD_LIBRARIES})

# object file
add_library(
  DINGODB_OBJS OBJECT
  ${COMMON_SRCS}
  ${CONFIG_SRCS}
  ${LOG_SRCS}
  ${REPORT_SRCS}
  ${VECTOR_SRCS}
  ${DOCUMENT_SRCS}
  ${SPLIT_SRCS}
  ${RAFT_SRCS}
  ${ENGINE_SRCS}
  ${CRONTAB_SRCS}
  ${HANDLER_SRCS}
  ${EVENT_SRCS}
  ${MVCC_SRCS}
  ${META_SRCS}
  ${COORDINATOR_SRCS}
  ${STORE_SRCS}
  ${SERVER_SRCS}
  ${SCAN_SRCS}
  ${METRICS_SRCS}
  ${SERIAL_SRCS}
  ${COPROCESSOR_SRCS}
  ${LIBEXPR_SRCS}
  ${DISKANN_SRCS}
  ${MERGE_SRCS}
  ${BR_SRCS})

if(NOT WITH_DISKANN)
  list(REMOVE_ITEM DINGODB_OBJS ${DISKANN_SRCS})
endif()

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

execute_process(
  COMMAND git describe --always --dirty
  OUTPUT_VARIABLE GIT_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git describe --abbrev=0 --tags --always
  OUTPUT_VARIABLE GIT_TAG_NAME
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log --pretty=format:%an -1
  OUTPUT_VARIABLE GIT_COMMIT_USER
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log --pretty=format:%ae -1
  OUTPUT_VARIABLE GIT_COMMIT_MAIL
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log --pretty=format:%ai -1
  OUTPUT_VARIABLE GIT_COMMIT_TIME
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT GIT_VERSION)
  set(GIT_VERSION "unknown")
  message(WARNING "Git version is unknown")
endif()

if(NOT GIT_TAG_NAME)
  set(GIT_TAG_NAME "unknown")
  message(WARNING "Git tag name is unknown")
endif()

message(STATUS "Git version: ${GIT_VERSION}")
message(STATUS "Git tag name: ${GIT_TAG_NAME}")
message(STATUS "Git commit user: ${GIT_COMMIT_USER}")
message(STATUS "Git commit mail: ${GIT_COMMIT_MAIL}")
message(STATUS "Git commit time: ${GIT_COMMIT_TIME}")

add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT=ON)
add_definitions(-DGIT_VERSION="${GIT_VERSION}")
add_definitions(-DGIT_TAG_NAME="${GIT_TAG_NAME}")
add_definitions(-DGIT_COMMIT_USER="${GIT_COMMIT_USER}")
add_definitions(-DGIT_COMMIT_MAIL="${GIT_COMMIT_MAIL}")
add_definitions(-DGIT_COMMIT_TIME="${GIT_COMMIT_TIME}")
add_definitions(-DDINGO_BUILD_TYPE="${CMAKE_BUILD_TYPE}")
add_definitions(-DDINGO_CONTRIB_BUILD_TYPE="${THIRD_PARTY_BUILD_TYPE}")

if(ENABLE_FAILPOINT)
  message(STATUS "Enable failpoint")
  add_definitions(-DENABLE_FAILPOINT="ON")
  unset(ENABLE_FAILPOINT CACHE)
endif()

add_executable(dingodb_server src/server/main.cc $<TARGET_OBJECTS:DINGODB_OBJS> $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(
  dingodb_client
  ${CLIENT_SRCS}
  src/coordinator/coordinator_interaction.cc
  src/common/role.cc
  src/common/helper.cc
  src/common/serial_helper.cc
  src/common/service_access.cc
  src/coprocessor/utils.cc
  src/mvcc/codec.cc
  src/vector/codec.cc
  src/document/codec.cc
  ${SERIAL_SRCS}
  ${VERSION_SRCS}
  $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(
  dingodb_cli
  ${CLIENT_V2_SRCS}
  src/coordinator/coordinator_interaction.cc
  src/common/role.cc
  src/common/helper.cc
  src/common/serial_helper.cc
  src/common/service_access.cc
  src/coprocessor/utils.cc
  src/mvcc/codec.cc
  src/vector/codec.cc
  src/document/codec.cc
  ${SERIAL_SRCS}
  ${VERSION_SRCS}
  $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(
  dingodb_br
  src/br/main.cc
  ${BR_SRCS}
  src/coordinator/coordinator_interaction.cc
  src/common/role.cc
  src/common/helper.cc
  src/common/serial_helper.cc
  src/common/service_access.cc
  src/coprocessor/utils.cc
  src/mvcc/codec.cc
  src/vector/codec.cc
  src/document/codec.cc
  src/common/synchronization.cc
  src/common/uuid.cc
  src/common/version.cc
  ${SERIAL_SRCS}
  ${VERSION_SRCS}
  $<TARGET_OBJECTS:PROTO_OBJS>)

add_dependencies(DINGODB_OBJS ${DEPEND_LIBS} ${PROTO_OBJS})
add_dependencies(dingodb_server ${DEPEND_LIBS})
add_dependencies(dingodb_client ${DEPEND_LIBS})
add_dependencies(dingodb_cli ${DEPEND_LIBS})
add_dependencies(dingodb_br ${DEPEND_LIBS})

if(DINGO_BUILD_STATIC)
  message(STATUS "Build DingoDB with static libraries linking")
  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    # This is only for build some modules for testing, not for the whole project. This project is currently not
    # compatible with MacOS.
  else()
    if(BUILD_GOOGLE_SANITIZE)
      set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++  -static-libasan")
    else()
      set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
    endif()
  endif()
else()
  if(BUILD_GOOGLE_SANITIZE)
    set(CMAKE_EXE_LINKER_FLAGS "-static-libasan")
  endif()
endif()
add_subdirectory(contrib/cli11)
target_link_libraries(dingodb_server ${DYNAMIC_LIB} ${VECTOR_LIB} "-Xlinker \"-(\"" ${BLAS_LIBRARIES} "-Xlinker \"-)\"")

target_link_libraries(dingodb_client ${DYNAMIC_LIB})

target_link_libraries(dingodb_cli ${DYNAMIC_LIB} CLI11::CLI11)

target_link_libraries(dingodb_br ${DYNAMIC_LIB})

if(BUILD_UNIT_TESTS)
  message(STATUS "Build unit test")
  add_subdirectory(test/unit_test)
endif()
