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
message(STATUS "Include glog...")

SET(GLOG_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/glog)
SET(GLOG_BINARY_DIR ${THIRD_PARTY_PATH}/build/glog)
SET(GLOG_INSTALL_DIR ${THIRD_PARTY_PATH}/install/glog)
SET(GLOG_INCLUDE_DIR "${GLOG_INSTALL_DIR}/include" CACHE PATH "glog include directory." FORCE)

IF (WIN32)
    SET(GLOG_LIBRARIES "${GLOG_INSTALL_DIR}/lib/glog.lib" CACHE FILEPATH "glog library." FORCE)
    SET(GLOG_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4267 /wd4530")
ELSE (WIN32)
    SET(GLOG_LIBRARIES "${GLOG_INSTALL_DIR}/lib/libglog${CMAKE_STATIC_LIBRARY_SUFFIX}" CACHE FILEPATH "glog library." FORCE)
    SET(GLOG_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
ENDIF (WIN32)

set(prefix_path "${THIRD_PARTY_PATH}/install/gflags")

SET(gflags_BUILD_STATIC_LIBS ON)

ExternalProject_Add(
    extern_glog
    ${EXTERNAL_PROJECT_LOG_ARGS}

    DEPENDS gflags

    SOURCE_DIR ${GLOG_SOURCES_DIR}
    BINARY_DIR ${GLOG_BINARY_DIR}
    PREFIX ${GLOG_BINARY_DIR}

    UPDATE_COMMAND ""
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    #-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_FLAGS=${GLOG_CMAKE_CXX_FLAGS}
    -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
    -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
    # -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
    # -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
    # -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
    -DCMAKE_INSTALL_PREFIX=${GLOG_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${GLOG_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DWITH_CUSTOM_PREFIX=ON
    -DWITH_GFLAGS=ON
    -DBUILD_SHARED_LIBS=OFF
    -Dgflags_DIR=${GFLAGS_INSTALL_DIR}/lib/cmake/gflags
    -DBUILD_TESTING=OFF
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    ${EXTERNAL_OPTIONAL_ARGS}
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${GLOG_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${GLOG_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_LIBRARY(glog STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET glog PROPERTY IMPORTED_LOCATION ${GLOG_LIBRARIES})
ADD_DEPENDENCIES(glog extern_glog)
