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
message(STATUS "Include zstd...")

SET(ZSTD_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/zstd)
SET(ZSTD_BUILD_DIR ${THIRD_PARTY_PATH}/build/zstd)
SET(ZSTD_INSTALL_DIR ${THIRD_PARTY_PATH}/install/zstd)
SET(ZSTD_INCLUDE_DIR "${ZSTD_INSTALL_DIR}/include" CACHE PATH "zstd include directory." FORCE)
SET(ZSTD_LIBRARIES "${ZSTD_INSTALL_DIR}/lib/libzstd.a" CACHE FILEPATH "zstd library." FORCE)

FILE(WRITE ${ZSTD_BUILD_DIR}/copy_repo.sh
    "mkdir -p ${ZSTD_BUILD_DIR} && cp -rf ${ZSTD_SOURCES_DIR}/* ${ZSTD_BUILD_DIR}/")

execute_process(COMMAND sh ${ZSTD_BUILD_DIR}/copy_repo.sh)

ExternalProject_Add(
    extern_zstd
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${ZSTD_BUILD_DIR}
    PREFIX ${ZSTD_BUILD_DIR}

    UPDATE_COMMAND  ""
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND $(MAKE) MOREFLAGS=-fPIC
    INSTALL_COMMAND mkdir -p ${ZSTD_INSTALL_DIR}/lib ${ZSTD_INCLUDE_DIR}
        COMMAND cp ${ZSTD_BUILD_DIR}/lib/libzstd.a ${ZSTD_INSTALL_DIR}/lib
        COMMAND cp ${ZSTD_BUILD_DIR}/lib/zstd.h ${ZSTD_INCLUDE_DIR}
        COMMAND cp ${ZSTD_BUILD_DIR}/lib/zdict.h ${ZSTD_INCLUDE_DIR}
        COMMAND cp ${ZSTD_BUILD_DIR}/lib/zstd_errors.h ${ZSTD_INCLUDE_DIR}
)

ADD_LIBRARY(zstd STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET zstd PROPERTY IMPORTED_LOCATION ${ZSTD_LIBRARIES})
ADD_DEPENDENCIES(zstd extern_zstd)
