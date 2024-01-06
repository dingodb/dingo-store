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
message(STATUS "Include bdb...")

SET(BDB_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/bdb)
SET(BDB_BINARY_DIR ${THIRD_PARTY_PATH}/build/bdb)
SET(BDB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/bdb)
SET(BDB_INCLUDE_DIR "${BDB_INSTALL_DIR}/include" CACHE PATH "bdb include directory." FORCE)
SET(BDB_LIBRARIES "${BDB_INSTALL_DIR}/lib/libdb_cxx.a" CACHE FILEPATH "bdb library." FORCE)

if(THIRD_PARTY_BUILD_TYPE MATCHES "Debug")
    set(BDB_DEBUG_FLAG "--enable-debug")
elseif(THIRD_PARTY_BUILD_TYPE MATCHES "RelWithDebInfo")
    set(BDB_DEBUG_FLAG "--enable-debug")
endif()


ExternalProject_Add(
    extern_bdb
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${BDB_SOURCES_DIR}
    BINARY_DIR ${BDB_BINARY_DIR}
    PREFIX ${BDB_BINARY_DIR}
    CONFIGURE_COMMAND ${BDB_SOURCES_DIR}/dist/configure --prefix ${BDB_INSTALL_DIR} --enable-cxx --enable-shared=no ${BDB_DEBUG_FLAG}
    BUILD_COMMAND $(MAKE) libdb_cxx.a
    INSTALL_COMMAND mkdir -p ${BDB_INSTALL_DIR}/lib ${BDB_INSTALL_DIR}/include
        COMMAND cp ${THIRD_PARTY_PATH}/build/bdb/libdb_cxx.a ${BDB_INSTALL_DIR}/lib/
        COMMAND cp ${THIRD_PARTY_PATH}/build/bdb/db.h ${BDB_INSTALL_DIR}/include/
        COMMAND cp ${THIRD_PARTY_PATH}/build/bdb/db_cxx.h ${BDB_INSTALL_DIR}/include/
)

ADD_LIBRARY(bdb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET bdb PROPERTY IMPORTED_LOCATION ${BDB_LIBRARIES})
ADD_DEPENDENCIES(bdb extern_bdb)
