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

INCLUDE(ExternalProject)
message(STATUS "Include diskann...")

SET(DISKANN_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/diskann)
SET(DISKANN_BINARY_DIR ${THIRD_PARTY_PATH}/build/diskann)
SET(DISKANN_INSTALL_DIR ${THIRD_PARTY_PATH}/install/diskann)
SET(DISKANN_INCLUDE_DIR "${DISKANN_INSTALL_DIR}/include" CACHE PATH "diskann include directory." FORCE)
SET(DISKANN_LIBRARIES "${DISKANN_INSTALL_DIR}/lib/libdiskann.a" CACHE FILEPATH "diskann library." FORCE)

set(prefix_path ${BOOST_SEARCH_PATH})

message(STATUS "diskann search boost in ${BOOST_SEARCH_PATH}")

SET(DISKANN_BUILD_TYPE ${THIRD_PARTY_BUILD_TYPE})

if(THIRD_PARTY_BUILD_TYPE MATCHES "Debug")
    message(STATUS "diskann does not support Debug, use RelWithDebInfo instead")
    SET(DISKANN_BUILD_TYPE "RelWithDebInfo")
endif()

ExternalProject_Add(
    extern_diskann
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${DISKANN_SOURCES_DIR}
    BINARY_DIR ${DISKANN_BINARY_DIR}
    PREFIX ${DISKANN_BINARY_DIR}

    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${DISKANN_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${DISKANN_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${DISKANN_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${DISKANN_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${DISKANN_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${DISKANN_BUILD_TYPE}
    BUILD_COMMAND $(MAKE) diskann
    INSTALL_COMMAND mkdir -p ${DISKANN_INSTALL_DIR}/lib/ COMMAND cp ${DISKANN_BINARY_DIR}/src/libdiskann.a ${DISKANN_LIBRARIES} COMMAND cp -r ${DISKANN_SOURCES_DIR}/include ${DISKANN_INCLUDE_DIR}/
)

ADD_LIBRARY(diskann STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET diskann PROPERTY IMPORTED_LOCATION ${DISKANN_LIBRARIES})
ADD_DEPENDENCIES(diskann extern_diskann)
