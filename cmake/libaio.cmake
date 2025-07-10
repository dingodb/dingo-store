# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

include(ExternalProject)
message(STATUS "Include libaio...")

set(LIBAIO_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/libaio)
set(LIBAIO_BINARY_DIR ${THIRD_PARTY_PATH}/build/libaio)
set(LIBAIO_INSTALL_DIR ${THIRD_PARTY_PATH}/install/libaio)
set(LIBAIO_INCLUDE_DIR
    "${LIBAIO_INSTALL_DIR}/include"
    CACHE PATH "libaio include directory." FORCE)
set(LIBAIO_LIBRARIES
    "${LIBAIO_INSTALL_DIR}/lib/libaio.a"
    CACHE FILEPATH "libaio library." FORCE)

if(THIRD_PARTY_BUILD_TYPE MATCHES "Debug")
  set(LIBAIO_DEBUG_FLAG "--enable-debug")
elseif(THIRD_PARTY_BUILD_TYPE MATCHES "RelWithDebInfo")
  set(LIBAIO_DEBUG_FLAG "--enable-debug")
endif()

add_custom_target(
  clean_libaio
  COMMAND $(MAKE) -C ${LIBAIO_SOURCE_DIR} clean
  COMMENT
    "Cleaning libaio.............................................................................................................................................."
  message
  (
  STATUS
  "Cleaning libaio..............................................................................................................................................."
  ))

# option(FORCE_REBUILD_LIBAIO "Force rebuild libaio" ON)

ExternalProject_Add(
  extern_libaio
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${LIBAIO_SOURCES_DIR}
  # BINARY_DIR ${LIBAIO_BINARY_DIR}
  PREFIX ${LIBAIO_BINARY_DIR}
  BUILD_IN_SOURCE ON
  CONFIGURE_COMMAND
    ""
    # BUILD_COMMAND $(MAKE) install prefix=${LIBAIO_BINARY_DIR} includedir=${LIBAIO_BINARY_DIR}/include
    # libdir=${LIBAIO_BINARY_DIR}/lib BUILD_COMMAND $(MAKE) -C src lib BUILD_COMMAND $(MAKE)
    # --file=${LIBAIO_SOURCES_DIR}/Makefile CLEAN_COMMAND $(MAKE) -C ${LIBAIO_SOURCES_DIR} clean BUILD_COMMAND $(MAKE)
    # -C ${LIBAIO_SOURCES_DIR} clean # BUILD_PRE_COMMAND ${CMAKE_COMMAND} -E env $(MAKE) -C ${LIBAIO_SOURCES_DIR} clean
  BUILD_COMMAND $(MAKE) -C ${LIBAIO_SOURCES_DIR} clean && $(MAKE) --file=${LIBAIO_SOURCES_DIR}/Makefile install prefix=${LIBAIO_BINARY_DIR} includedir=${LIBAIO_BINARY_DIR}/include libdir=${LIBAIO_BINARY_DIR}/lib
  INSTALL_COMMAND mkdir -p ${LIBAIO_INSTALL_DIR}/lib ${LIBAIO_INSTALL_DIR}/include
  COMMAND cp ${LIBAIO_BINARY_DIR}/include/libaio.h ${LIBAIO_INSTALL_DIR}/include/
  COMMAND cp ${LIBAIO_BINARY_DIR}/lib/libaio.a ${LIBAIO_INSTALL_DIR}/lib/
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
  LOG_INSTALL ON)

add_library(libaio STATIC IMPORTED GLOBAL)
set_property(TARGET libaio PROPERTY IMPORTED_LOCATION ${LIBAIO_LIBRARIES})
add_dependencies(libaio clean_libaio extern_libaio)
# add_dependencies(libaio extern_libaio)
