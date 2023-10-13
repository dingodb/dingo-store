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

SET(GPERFTOOLS_SOURCES_DIR ${THIRD_PARTY_PATH}/gperftools)
SET(GPERFTOOLS_BINARY_DIR ${THIRD_PARTY_PATH}/build/gperftools)
SET(GPERFTOOLS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gperftools)
SET(GPERFTOOLS_INCLUDE_DIR "${GPERFTOOLS_INSTALL_DIR}/include" CACHE PATH "gperftools include directory." FORCE)
SET(GPERFTOOLS_LIBRARIES "${GPERFTOOLS_INSTALL_DIR}/lib/libtcmalloc_and_profiler.a" CACHE FILEPATH "gperftools library." FORCE)
SET(GPERFTOOLS_MINIMAL_LIBRARIES "${GPERFTOOLS_INSTALL_DIR}/lib/libtcmalloc_minimal.a" CACHE FILEPATH "gperftools library." FORCE)

if(BRPC_ENABLE_CPU_PROFILER)
    set(CONFIGURE_COMMAND_GPERF sh autogen.sh COMMAND sh ./configure --prefix=${GPERFTOOLS_INSTALL_DIR} --enable-shared=no --enable-static=yes --enable-libunwind --enable-cpu-profiler --enable-heap-profiler --enable--heap-checker CPPFLAGS=-I${THIRD_PARTY_PATH}/install/libunwind/include LDFLAGS=-L${THIRD_PARTY_PATH}/install/libunwind/lib CXXFLAGS=-g)
    message(STATUS "gperftools found: BRPC_ENABLE_CPU_PROFILER is enabled, enable all profiler")
    message(STATUS "${CONFIGURE_COMMAND_GPERF}")
else()
    set(CONFIGURE_COMMAND_GPERF sh autogen.sh COMMAND sh ./configure --prefix=${GPERFTOOLS_INSTALL_DIR} --enable-shared=no --enable-static=yes --enable-minimal CPPFLAGS=-I${THIRD_PARTY_PATH}/install/libunwind/include LDFLAGS=-L${THIRD_PARTY_PATH}/install/libunwind/lib CXXFLAGS=-g)
    message(STATUS "gperftools found: BRPC_ENABLE_CPU_PROFILER is disable, use --enable-minimal")
    message(STATUS "${CONFIGURE_COMMAND_GPERF}")
endif()

FILE(WRITE ${GPERFTOOLS_SOURCES_DIR}/src/copy_repo.sh
        "mkdir -p ${GPERFTOOLS_SOURCES_DIR}/src/extern_gperftools/ && cp -rf ${CMAKE_SOURCE_DIR}/contrib/gperftools/* ${GPERFTOOLS_SOURCES_DIR}/src/extern_gperftools/")

execute_process(COMMAND sh ${GPERFTOOLS_SOURCES_DIR}/src/copy_repo.sh)

ExternalProject_Add(
        extern_gperftools
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS libunwind
        SOURCE_DIR ${GPERFTOOLS_SOURCES_DIR}/src/extern_gperftools/
        # BINARY_DIR ${GPERFTOOLS_BINARY_DIR}
        PREFIX ${GPERFTOOLS_INSTALL_DIR}
        BUILD_IN_SOURCE 1
        # CONFIGURE_COMMAND sh autogen.sh COMMAND sh ./configure --prefix=${GPERFTOOLS_INSTALL_DIR} --enable-shared=no --enable-static=yes --enable-libunwind CPPFLAGS=-I${THIRD_PARTY_PATH}/install/libunwind/include LDFLAGS=-L${THIRD_PARTY_PATH}/install/libunwind/lib CXXFLAGS=-g
        CONFIGURE_COMMAND ${CONFIGURE_COMMAND_GPERF}
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install
)

ADD_LIBRARY(gperftools STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET gperftools PROPERTY IMPORTED_LOCATION ${GPERFTOOLS_LIBRARIES})
ADD_DEPENDENCIES(gperftools extern_gperftools)
