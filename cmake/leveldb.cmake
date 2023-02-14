# Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

INCLUDE(ExternalProject)

SET(LEVELDB_SOURCES_DIR ${THIRD_PARTY_PATH}/leveldb)
SET(LEVELDB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/leveldb)
SET(LEVELDB_INCLUDE_DIR "${LEVELDB_INSTALL_DIR}/include" CACHE PATH "leveldb include directory." FORCE)
SET(LEVELDB_LIBRARIES "${LEVELDB_INSTALL_DIR}/lib/libleveldb.a" CACHE FILEPATH "leveldb library." FORCE)

ExternalProject_Add(
        extern_leveldb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS gflags zlib snappy
        PREFIX ${leveldb_SOURCES_DIR}
        # GIT_REPOSITORY "https://github.com/google/leveldb"
        # GIT_TAG "1.23"
        URL "https://github.com/google/leveldb/archive/1.23.tar.gz"
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DLEVELDB_BUILD_TESTS=OFF
        -DLEVELDB_BUILD_BENCHMARKS=OFF
        # -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        # -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
        # -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
        # -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        # -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
        # -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
        -DCMAKE_INSTALL_PREFIX=${LEVELDB_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${LEVELDB_INSTALL_DIR}/lib
        -DCMAKE_INSTALL_LIBDIR:PATH=${LEVELDB_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        # -DPORTABLE=ON
        # -DWITH_SNAPPY=ON
        # -DWITH_RUNTIME_DEBUG=ON
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${LEVELDB_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_LIBRARY(leveldb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET leveldb PROPERTY IMPORTED_LOCATION ${LEVELDB_LIBRARIES})
ADD_DEPENDENCIES(leveldb extern_leveldb)
