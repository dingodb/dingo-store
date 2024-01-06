# Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
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

INCLUDE(ExternalProject)
message(STATUS "Include rocksdb...")

SET(ROCKSDB_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/rocksdb)
SET(ROCKSDB_BINARY_DIR ${THIRD_PARTY_PATH}/build/rocksdb)
SET(ROCKSDB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/rocksdb)
SET(ROCKSDB_INCLUDE_DIR "${ROCKSDB_INSTALL_DIR}/include" CACHE PATH "rocksdb include directory." FORCE)
SET(ROCKSDB_LIBRARIES "${ROCKSDB_INSTALL_DIR}/lib/librocksdb.a" CACHE FILEPATH "rocksdb library." FORCE)

set(prefix_path "${THIRD_PARTY_PATH}/install/snappy|${THIRD_PARTY_PATH}/install/zlib|${THIRD_PARTY_PATH}/install/lz4|${THIRD_PARTY_PATH}/install/zstd|${THIRD_PARTY_PATH}/install/gflags")

# To avoid rocksdb PROTABLE options on old arch machine when build using docker
set(ROCKSDB_PROTABLE_OPTION "0" CACHE STRING "An option for rocksdb PROTABLE, default 0")
message(STATUS "Rocksdb protable option(ROCKSDB_PROTABLE_OPTION): ${ROCKSDB_PROTABLE_OPTION}")

ExternalProject_Add(
    extern_rocksdb
    ${EXTERNAL_PROJECT_LOG_ARGS}

    DEPENDS gflags zlib snappy lz4 zstd

    SOURCE_DIR ${ROCKSDB_SOURCES_DIR}
    BINARY_DIR ${ROCKSDB_BINARY_DIR}
    PREFIX ${ROCKSDB_BINARY_DIR}

    UPDATE_COMMAND ""
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
    -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
    -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
    -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
    -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
    -DCMAKE_INSTALL_PREFIX=${ROCKSDB_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${ROCKSDB_INSTALL_DIR}/lib
    -DCMAKE_INSTALL_LIBDIR:PATH=${ROCKSDB_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    -DPORTABLE=${ROCKSDB_PROTABLE_OPTION} # Minimum CPU arch to support, or 0 = current CPU, 1 = baseline CPU
    -DWITH_SNAPPY=ON
    -DWITH_LZ4=ON
    -DWITH_ZSTD=ON
    -DWITH_ZLIB=ON
    -DWITH_RUNTIME_DEBUG=ON
    -DROCKSDB_BUILD_SHARED=OFF
    -DWITH_BENCHMARK_TOOLS=OFF
    -DWITH_TESTS=OFF
    -DWITH_CORE_TOOLS=OFF
    -DWITH_TOOLS=OFF
    -DWITH_TRACE_TOOLS=OFF
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${ROCKSDB_INSTALL_DIR}
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_LIBRARY(rocksdb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${ROCKSDB_LIBRARIES})
ADD_DEPENDENCIES(rocksdb extern_rocksdb)
