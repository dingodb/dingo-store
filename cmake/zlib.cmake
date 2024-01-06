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
message(STATUS "Include zlib...")

SET(ZLIB_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/zlib)
SET(ZLIB_BINARY_DIR ${THIRD_PARTY_PATH}/build/zlib)
SET(ZLIB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/zlib)
SET(ZLIB_ROOT ${ZLIB_INSTALL_DIR} CACHE FILEPATH "zlib root directory." FORCE)
SET(ZLIB_INCLUDE_DIR "${ZLIB_INSTALL_DIR}/include" CACHE PATH "zlib include directory." FORCE)
SET(ZLIB_LIBRARIES "${ZLIB_INSTALL_DIR}/lib/libz.a" CACHE FILEPATH "zlib library." FORCE)

FILE(WRITE ${ZLIB_BINARY_DIR}/copy_repo.sh
    "mkdir -p ${ZLIB_BINARY_DIR} && cp -rf ${ZLIB_SOURCES_DIR}/* ${ZLIB_BINARY_DIR}/")

execute_process(COMMAND sh ${ZLIB_BINARY_DIR}/copy_repo.sh)

ExternalProject_Add(
    extern_zlib
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${ZLIB_BINARY_DIR}
    BINARY_DIR ${ZLIB_BINARY_DIR}
    PREFIX ${ZLIB_INSTALL_DIR}

    UPDATE_COMMAND  ""
    CMAKE_ARGS
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${ZLIB_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${ZLIB_INSTALL_DIR}/lib
    -DBUILD_SHARED_LIBS=OFF
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_MACOSX_RPATH=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${ZLIB_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${ZLIB_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_LIBRARY(zlib STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET zlib PROPERTY IMPORTED_LOCATION ${ZLIB_LIBRARIES})
ADD_DEPENDENCIES(zlib extern_zlib)
