# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

include(ExternalProject)
message(STATUS "Include gperftool...")

set(GPERFTOOLS_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/gperftools)
set(GPERFTOOLS_BUILD_DIR ${THIRD_PARTY_PATH}/build/gperftools)
set(GPERFTOOLS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gperftools)
set(GPERFTOOLS_INCLUDE_DIR
    "${GPERFTOOLS_INSTALL_DIR}/include"
    CACHE PATH "gperftools include directory." FORCE)
set(GPERFTOOLS_LIBRARIES
    "${GPERFTOOLS_INSTALL_DIR}/lib/libtcmalloc_and_profiler.a"
    CACHE FILEPATH "gperftools library." FORCE)
set(GPERFTOOLS_MINIMAL_LIBRARIES
    "${GPERFTOOLS_INSTALL_DIR}/lib/libtcmalloc_minimal.a"
    CACHE FILEPATH "gperftools library." FORCE)

set(CONFIGURE_COMMAND_GPERF
    sh autoreconf -i -f COMMAND patch --forward -t --reject-file=-
    m4/libtool.m4 m4/libtool.patch COMMAND sh autoreconf -i COMMAND sh
    configure --prefix=${GPERFTOOLS_INSTALL_DIR} --enable-shared=no
    --enable-static=yes --with-pic --enable-libunwind --enable-cpu-profiler
    --enable-heap-profiler --enable-heap-checker
    CPPFLAGS=-I${THIRD_PARTY_PATH}/install/libunwind/include
    LDFLAGS=-L${THIRD_PARTY_PATH}/install/libunwind/lib CXXFLAGS=-g)
message(STATUS "gperftools enable all profiler")

file(
  WRITE ${GPERFTOOLS_BUILD_DIR}/copy_repo.sh
  "mkdir -p ${GPERFTOOLS_BUILD_DIR} && cp -rf ${GPERFTOOLS_SOURCES_DIR}/* ${GPERFTOOLS_BUILD_DIR}/"
)

execute_process(COMMAND sh ${GPERFTOOLS_BUILD_DIR}/copy_repo.sh)

ExternalProject_Add(
  extern_gperftools
  ${EXTERNAL_PROJECT_LOG_ARGS}
  DEPENDS libunwind
  SOURCE_DIR ${GPERFTOOLS_BUILD_DIR}
  PREFIX ${GPERFTOOLS_BUILD_DIR}
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND ${CONFIGURE_COMMAND_GPERF}
  BUILD_COMMAND $(MAKE)
  INSTALL_COMMAND $(MAKE) install)

add_library(gperftools STATIC IMPORTED GLOBAL)
set_property(TARGET gperftools PROPERTY IMPORTED_LOCATION
                                        ${GPERFTOOLS_LIBRARIES})
add_dependencies(gperftools extern_gperftools)
