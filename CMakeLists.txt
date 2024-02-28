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

# Note: if BRPC_ENABLE_CPU_PROFILER is ON, there may be child process hang because of the sample process of tcmalloc will cause dead lock after fork,
#       so do not enable DBRPC_ENABLE_CPU_PROFILER in production enviroment.
# For develop without MKL:
#     cmake .. -DCMAKE_BUILD_TYPE=Debug -DTHIRD_PARTY_BUILD_TYPE=Debug -DBRPC_ENABLE_CPU_PROFILER=ON -DLINK_TCMALLOC=ON
# For develop with MKL:
#     cmake .. -DCMAKE_BUILD_TYPE=Debug -DTHIRD_PARTY_BUILD_TYPE=Debug -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=ON -DLINK_TCMALLOC=ON
# For RelWithDbgInfo:
#     cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTHIRD_PARTY_BUILD_TYPE=RelWithDebInfo -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=OFF -DLINK_TCMALLOC=ON
# For RelWithDbgInfo:
#     cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTHIRD_PARTY_BUILD_TYPE=RelWithDebInfo -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=OFF -DLINK_TCMALLOC=ON
# For Release:
#     cmake .. -DCMAKE_BUILD_TYPE=Release -DTHIRD_PARTY_BUILD_TYPE=Release -DWITH_MKL=ON -DBRPC_ENABLE_CPU_PROFILER=OFF -DLINK_TCMALLOC=ON
# Build all binaries include unit test:
#     make
# Build only product binaries:
#     make dingodb_server dingodb_client

# if compile_commands.json is needed, please enable CMAKE_EXPORT_COMPILE_COMMANDS, of use `bear --append -- make` to do make, it's more recommended to use bear.

cmake_minimum_required(VERSION 3.23.1 FATAL_ERROR)
project(dingo-store C CXX)

option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(LINK_TCMALLOC "Link tcmalloc if possible" OFF)
option(BUILD_UNIT_TESTS "Build unit test" OFF)
option(ENABLE_COVERAGE "Enable unit test code coverage" OFF)
option(BUILD_INTEGRATION_TESTS "Build integration test" OFF)
option(BUILD_BENCHMARK "Build benchmark" OFF)
option(BUILD_SDK_EXAMPLE "Build sdk example" OFF)
option(DINGO_BUILD_STATIC "Link libraries statically to generate the dingodb binary" ON)
option(ENABLE_FAILPOINT "Enable failpoint" OFF)
option(WITH_DISKANN "Build with diskann index" OFF)
option(WITH_MKL "Build with intel mkl" OFF)
option(BOOST_SEARCH_PATH "")
option(BUILD_GOOGLE_SANITIZE "Enable google sanitize" OFF)
option(BRPC_ENABLE_CPU_PROFILER "Enable brpc cpu profiler" OFF)
option(XDPROCKS_PATH "Enable xdprocks raw engine")
option(BUILD_PYTHON_SDK "Build python sdk" OFF)

message(STATUS CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
message(STATUS THIRD_PARTY_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE})

SET(CMAKE_C_FLAGS_DEBUG "$ENV{CFLAGS} -O0  -g3 -ggdb")
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0  -g3 -ggdb")

message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")

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

    execute_process(
        COMMAND sh ${CMAKE_CURRENT_SOURCE_DIR}/scripts/prepare_xdprocks.sh --xdprocks_dir ${XDPROCKS_PATH}
    )
else()
    message(STATUS "Disable xdprocks")
    set(ENABLE_XDPROCKS OFF)
endif()

if(BRPC_ENABLE_CPU_PROFILER)
    message(STATUS "Enable BRPC_ENABLE_CPU_PROFILER")
    add_definitions(-DBRPC_ENABLE_CPU_PROFILER=ON)
    set(LINK_TCMALLOC ON)
endif()

set (CMAKE_POSITION_INDEPENDENT_CODE ON)

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

include(CheckCXXCompilerFlag)
if (CMAKE_CXX_STANDARD EQUAL 23)
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
    message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD} change to c++17")
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

if (CMAKE_CXX_STANDARD EQUAL 23)
    set(CMAKE_CXX_STANDARD 23)
elseif(CMAKE_CXX_STANDARD EQUAL 20)
    set(CMAKE_CXX_STANDARD 20)
else()
    set(CMAKE_CXX_STANDARD 17)
endif()
# set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(THIRD_PARTY_PATH ${CMAKE_CURRENT_BINARY_DIR}/third-party)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

execute_process(COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/contrib/.clang-tidy ${CMAKE_CURRENT_BINARY_DIR}/)

find_package(Threads REQUIRED)

find_package(OpenMP REQUIRED)

execute_process(
    COMMAND g++ --print-file-name libgomp.a
    RESULT_VARIABLE CMD_RESULT
    OUTPUT_VARIABLE FILE_NAME
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

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

include(gflags)
include(lz4)
include(zstd)
include(zlib)
include(snappy)
include(openssl)
include(glog)
include(gtest)
include(fmt)
include(protobuf)
include(yaml-cpp)
include(libunwind)
include(libbacktrace)
include(hnswlib)
include(rapidjson)
include(leveldb)
include(rocksdb)
include(bdb)
include(brpc)
include(braft)

if(BUILD_BENCHMARK STREQUAL "ON")
    include(hdf5)
endif()

message(STATUS "protoc: ${PROTOBUF_PROTOC_EXECUTABLE}")
message(STATUS "protoc lib: ${PROTOBUF_PROTOC_LIBRARY}")
message(STATUS "protobuf include: ${PROTOBUF_INCLUDE_DIR}")
message(STATUS "protobuf lib: ${PROTOBUF_LIBRARIES}")
message(STATUS "protobuf lite lib: ${PROTOBUF_LITE_LIBRARY}")

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

        message(STATUS "proto file: ${msg}")

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
set_target_properties(PROTO_OBJS
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON)
add_custom_target(build_proto ALL
                DEPENDS ${PROTO_SRCS} ${PROTO_HDRS}
                COMMENT "generate message target"
                VERBATIM
                )

# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${ZSTD_INCLUDE_DIR})
include_directories(${ZLIB_INCLUDE_DIR})
include_directories(${BRAFT_INCLUDE_DIR})
include_directories(${BRPC_INCLUDE_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${GTEST_INCLUDE_DIR})
include_directories(${GMOCK_INCLUDE_DIR})
include_directories(${GFLAGS_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIR})
include_directories(${BDB_INCLUDE_DIR})
include_directories(${RAPIDJSON_INCLUDE_DIR})
if (ENABLE_XDPROCKS)
    include_directories(${XDPROCKS_PATH}/include)
endif()
include_directories(${YAMLCPP_INCLUDE_DIR})
include_directories(${FMT_INCLUDE_DIR})
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${LIBUNWIND_INCLUDE_DIR})
include_directories(${LIBBACKTRACE_INCLUDE_DIR})
include_directories(${HDF5_INCLUDE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/src)

# CAUTION: the order of the libraries is important, please do not change it.
set(DYNAMIC_LIB
    ${BRAFT_LIBRARIES}
    ${BRPC_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LEVELDB_LIBRARIES}
    ${ROCKSDB_LIBRARIES}
    ${SNAPPY_LIBRARIES}
    ${LZ4_LIBRARIES}
    ${ZSTD_LIBRARIES}
    ${YAMLCPP_LIBRARIES}
    ${FMT_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${GTEST_LIBRARIES}
    ${LIBUNWIND_LIBRARIES}
    ${LIBUNWIND_GENERIC_LIBRARIES}
    ${LIBUNWIND_ARCH_LIBRARIES}
    ${LIBBACKTRACE_LIBRARIES}
    ${BDB_LIBRARIES}
    )

if (ENABLE_XDPROCKS)
    set(DYNAMIC_LIB
        ${DYNAMIC_LIB}
        "${XDPROCKS_PATH}/lib/libxdprocks.a"
        "${XDPROCKS_PATH}/lib/libstorelib.a"
        "${XDPROCKS_PATH}/lib/libzlog.a"
        "-l:libpci.so.3"
        "-l:libudev.so.1"
    )
endif()

set(DYNAMIC_LIB
    ${DYNAMIC_LIB}
    dl
    Threads::Threads
)

if(ENABLE_COVERAGE)
    set(DYNAMIC_LIB
        ${DYNAMIC_LIB}
        gcov
    )
endif()

set(DEPEND_LIBS
    openssl
    crypto
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
    faiss
    fmt
    gtest
    libunwind
    libbacktrace
    bdb
    rapidjson
    )

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

    find_path(MKL_ROOT include/mkl.h PATHS ${MKLROOT} DOC "Folder contains MKL")
    find_path(MKL_INCLUDE_DIR NAMES mkl.h HINTS ${MKL_ROOT}/include)

    find_library(MKL_INTERFACE_LIBRARY
             NAMES ${INT_LIB}
             PATHS ${MKL_ROOT}/lib/intel64)

    find_library(MKL_SEQUENTIAL_LAYER_LIBRARY
             NAMES ${SEQ_LIB}
             PATHS ${MKL_ROOT}/lib/intel64)

    find_library(MKL_CORE_LIBRARY
             NAMES ${COR_LIB}
             PATHS ${MKL_ROOT}/lib/intel64)

    set(MKL_LIBRARIES ${MKL_SEQUENTIAL_LAYER_LIBRARY} ${MKL_INTERFACE_LIBRARY} ${MKL_CORE_LIBRARY})
    set(BLAS_LIBRARIES ${MKL_LIBRARIES})

    message(STATUS "MKL_LIBRARIES=${MKL_LIBRARIES}")

    add_definitions(-DUSE_MKL=ON)

    include_directories(${MKL_INCLUDE_DIR})
    include(faiss-mkl)

else()
    # find_package(MKL QUIET)
    # if(MKL_FOUND)
    #     message(FATAL_ERROR "The MKL is found, cannot build faiss with openblas, please disable MKL.")
    # endif()

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
    if(NOT WITH_MKL)
        message(FATAL_ERROR "The WITH_MKL is not ON, please install enable WITH_MKL to build diskann.")
    endif()

    if(NOT MKL_FOUND)
        message(FATAL_ERROR "The MKL is not found, please install intel mkl to build diskann.")
    endif()

    if(NOT BOOST_SEARCH_PATH)
        find_package(Boost REQUIRED COMPONENTS program_options)
    else()
        message(STATUS "BOOST_SEARCH_PATH=${BOOST_SEARCH_PATH}, use user-defined boost version")
    endif()

    include(diskann)
    include_directories(${DISKANN_INCLUDE_DIR})
    set(DEPEND_LIBS ${DEPEND_LIBS} diskann)
    set(VECTOR_LIB {VECTOR_LIB} ${DISKANN_LIBRARIES})
endif()

if(LINK_TCMALLOC)
    add_definitions(-DLINK_TCMALLOC=ON)

    message(STATUS "Build DingoDB with tcmalloc")

    include(gperftools)
    include_directories(${GPERFTOOLS_INCLUDE_DIR})

    if(BRPC_ENABLE_CPU_PROFILER)
        set(DYNAMIC_LIB ${DYNAMIC_LIB} ${GPERFTOOLS_LIBRARIES})
    else()
        set(DYNAMIC_LIB ${DYNAMIC_LIB} ${GPERFTOOLS_MINIMAL_LIBRARIES})
    endif()
    set(DEPEND_LIBS ${DEPEND_LIBS} gperftools)
endif()

# source file
file(GLOB COMMON_SRCS ${PROJECT_SOURCE_DIR}/src/common/*.cc)
file(GLOB CONFIG_SRCS ${PROJECT_SOURCE_DIR}/src/config/*.cc)
file(GLOB LOG_SRCS ${PROJECT_SOURCE_DIR}/src/log/*.cc)
file(GLOB REPORT_SRCS ${PROJECT_SOURCE_DIR}/src/report/*.cc)
file(GLOB VECTOR_SRCS ${PROJECT_SOURCE_DIR}/src/vector/*.cc)
file(GLOB SPLIT_SRCS ${PROJECT_SOURCE_DIR}/src/split/*.cc)
file(GLOB RAFT_SRCS ${PROJECT_SOURCE_DIR}/src/raft/*.cc)
file(GLOB ENGINE_SRCS ${PROJECT_SOURCE_DIR}/src/engine/*.cc)
file(GLOB CRONTAB_SRCS ${PROJECT_SOURCE_DIR}/src/crontab/*.cc)
file(GLOB HANDLER_SRCS ${PROJECT_SOURCE_DIR}/src/handler/*.cc)
file(GLOB EVENT_SRCS ${PROJECT_SOURCE_DIR}/src/event/*.cc)
file(GLOB META_SRCS ${PROJECT_SOURCE_DIR}/src/meta/*.cc)
file(GLOB COORDINATOR_SRCS ${PROJECT_SOURCE_DIR}/src/coordinator/*.cc)
file(GLOB STORE_SRCS ${PROJECT_SOURCE_DIR}/src/store/*.cc)
file(GLOB SERVER_SRCS ${PROJECT_SOURCE_DIR}/src/server/*.cc)
file(GLOB SCAN_SRCS ${PROJECT_SOURCE_DIR}/src/scan/*.cc)
file(GLOB METRICS_SRCS ${PROJECT_SOURCE_DIR}/src/metrics/*.cc)
file(GLOB VERSION_SRCS ${PROJECT_SOURCE_DIR}/src/common/version.cc)
file(GLOB SERIAL1_SRCS ${PROJECT_SOURCE_DIR}/src/serial/*.cc)
file(GLOB SERIAL2_SRCS ${PROJECT_SOURCE_DIR}/src/serial/schema/*.cc)
file(GLOB_RECURSE LIBEXPR_SRCS ${PROJECT_SOURCE_DIR}/src/libexpr/src/*.cc)
file(GLOB COPROCESSOR_SRCS ${PROJECT_SOURCE_DIR}/src/coprocessor/*.cc)
file(GLOB CLIENT_SRCS ${PROJECT_SOURCE_DIR}/src/client/*.cc)

if (NOT ENABLE_XDPROCKS)
    list(REMOVE_ITEM ENGINE_SRCS "${PROJECT_SOURCE_DIR}/src/engine/xdprocks_raw_engine.cc")
endif()

list(REMOVE_ITEM SERVER_SRCS "${PROJECT_SOURCE_DIR}/src/server/main.cc")

# object file
add_library(DINGODB_OBJS
            OBJECT
            ${COMMON_SRCS}
            ${CONFIG_SRCS}
            ${LOG_SRCS}
            ${REPORT_SRCS}
            ${VECTOR_SRCS}
            ${SPLIT_SRCS}
            ${RAFT_SRCS}
            ${ENGINE_SRCS}
            ${CRONTAB_SRCS}
            ${HANDLER_SRCS}
            ${EVENT_SRCS}
            ${META_SRCS}
            ${COORDINATOR_SRCS}
            ${STORE_SRCS}
            ${SERVER_SRCS}
            ${SCAN_SRCS}
            ${METRICS_SRCS}
            ${SERIAL1_SRCS}
            ${SERIAL2_SRCS}
            ${COPROCESSOR_SRCS}
            ${LIBEXPR_SRCS}
            )

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

execute_process(
    COMMAND git describe --always --dirty
    OUTPUT_VARIABLE GIT_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND git describe --abbrev=0 --tags --always
    OUTPUT_VARIABLE GIT_TAG_NAME
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND git log --pretty=format:%an -1
    OUTPUT_VARIABLE GIT_COMMIT_USER
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND git log --pretty=format:%ae -1
    OUTPUT_VARIABLE GIT_COMMIT_MAIL
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND git log --pretty=format:%ai -1
    OUTPUT_VARIABLE GIT_COMMIT_TIME
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (NOT GIT_VERSION)
    set(GIT_VERSION "unknown")
endif()

if (NOT GIT_TAG_NAME)
    set(GIT_TAG_NAME "unknown")
endif()

add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT=ON)
add_definitions(-DGIT_VERSION="${GIT_VERSION}")
add_definitions(-DGIT_TAG_NAME="${GIT_TAG_NAME}")
add_definitions(-DGIT_COMMIT_USER="${GIT_COMMIT_USER}")
add_definitions(-DGIT_COMMIT_MAIL="${GIT_COMMIT_MAIL}")
add_definitions(-DGIT_COMMIT_TIME="${GIT_COMMIT_TIME}")
add_definitions(-DDINGO_BUILD_TYPE="${CMAKE_BUILD_TYPE}")
add_definitions(-DDINGO_CONTRIB_BUILD_TYPE="${THIRD_PARTY_BUILD_TYPE}")

if (ENABLE_FAILPOINT)
    message(STATUS "Enable failpoint")
    add_definitions(-DENABLE_FAILPOINT="ON")
    unset(ENABLE_FAILPOINT CACHE)
endif()

add_executable(dingodb_server src/server/main.cc $<TARGET_OBJECTS:DINGODB_OBJS> $<TARGET_OBJECTS:PROTO_OBJS>)
add_executable(dingodb_client
                ${CLIENT_SRCS}
                src/coordinator/coordinator_interaction.cc
                src/common/role.cc
                src/common/helper.cc
                src/common/service_access.cc
                src/coprocessor/utils.cc
                src/vector/codec.cc
                ${SERIAL1_SRCS}
                ${SERIAL2_SRCS}
                ${VERSION_SRCS} $<TARGET_OBJECTS:PROTO_OBJS>)

add_dependencies(DINGODB_OBJS ${DEPEND_LIBS})
add_dependencies(dingodb_server ${DEPEND_LIBS})
add_dependencies(dingodb_client ${DEPEND_LIBS})

if(DINGO_BUILD_STATIC)
    message(STATUS "Build DingoDB with static libraries linking")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
        # This is only for build some modules for testing, not for the whole project.
        # This project is currently not compatible with MacOS.
    else()
        if(BUILD_GOOGLE_SANITIZE)
            set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++  -static-libasan")
        else()
            set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
        endif()
    endif()
else ()
    if(BUILD_GOOGLE_SANITIZE)
        set(CMAKE_EXE_LINKER_FLAGS "-static-libasan")
    endif()
endif()

target_link_libraries(dingodb_server
                      ${DYNAMIC_LIB}
                      ${VECTOR_LIB}
                      "-Xlinker \"-(\""
                      ${BLAS_LIBRARIES}
                      "-Xlinker \"-)\"")

target_link_libraries(dingodb_client
                      ${DYNAMIC_LIB})

add_subdirectory(src/sdk)

if(BUILD_SDK_EXAMPLE)
    message(STATUS "Build sdk example")
    add_subdirectory(src/example)
endif()

if(BUILD_UNIT_TESTS)
    message(STATUS "Build unit test")
    add_subdirectory(test/unit_test)
endif()

if(BUILD_INTEGRATION_TESTS)
    message(STATUS "Build integration test")
    add_subdirectory(test/integration_test)
endif()

if(BUILD_BENCHMARK)
    message(STATUS "Build benchmark")
    add_subdirectory(src/benchmark)
endif()

if(BUILD_PYTHON_SDK)
    message(STATUS "Build python sdk")
    add_subdirectory(src/pysdk)
endif()