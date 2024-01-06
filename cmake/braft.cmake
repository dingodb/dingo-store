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
message(STATUS "Include braft...")

SET(BRAFT_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/braft)
SET(BRAFT_BINARY_DIR ${THIRD_PARTY_PATH}/build/braft)
SET(BRAFT_INSTALL_DIR ${THIRD_PARTY_PATH}/install/braft)
SET(BRAFT_INCLUDE_DIR "${BRAFT_INSTALL_DIR}/include" CACHE PATH "braft include directory." FORCE)
SET(BRAFT_LIBRARIES "${BRAFT_INSTALL_DIR}/lib/libbraft.a" CACHE FILEPATH "braft library." FORCE)


set(prefix_path "${THIRD_PARTY_PATH}/install/brpc|${THIRD_PARTY_PATH}/install/gflags|${THIRD_PARTY_PATH}/install/protobuf|${THIRD_PARTY_PATH}/install/zlib|${THIRD_PARTY_PATH}/install/glog|${THIRD_PARTY_PATH}/install/leveldb")

set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -DUSE_BTHREAD_MUTEX)

ExternalProject_Add(
    extern_braft
    ${EXTERNAL_PROJECT_LOG_ARGS}

    DEPENDS brpc

    SOURCE_DIR ${BRAFT_SOURCES_DIR}
    BINARY_DIR ${BRAFT_BINARY_DIR}
    PREFIX ${BRAFT_BINARY_DIR}

    UPDATE_COMMAND ""
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${BRAFT_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${BRAFT_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    -DBRPC_WITH_GLOG=ON
    -DGLOG_INCLUDE_PATH=${GLOG_INCLUDE_DIR}
    -DGLOG_LIB=${GLOG_LIBRARIES}
    -DGFLAGS_INCLUDE_PATH=${GFLAGS_INCLUDE_DIR}
    -DGFLAGS_LIB=${GFLAGS_LIBRARIES}
    -DWITH_DEBUG_SYMBOLS=OFF
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${BRAFT_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${BRAFT_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
    BUILD_COMMAND $(MAKE) braft-static
    INSTALL_COMMAND mkdir -p ${BRAFT_INSTALL_DIR}/lib/ COMMAND cp ${BRAFT_BINARY_DIR}/output/lib/libbraft.a ${BRAFT_LIBRARIES} COMMAND cp -r ${BRAFT_BINARY_DIR}/output/include ${BRAFT_INCLUDE_DIR}/
)

ADD_LIBRARY(braft STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET braft PROPERTY IMPORTED_LOCATION ${BRAFT_LIBRARIES})
ADD_DEPENDENCIES(braft extern_braft)
