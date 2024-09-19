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
message(STATUS "Include liburing...")

set(LIBURING_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/liburing)
set(LIBURING_BINARY_DIR ${THIRD_PARTY_PATH}/build/liburing)
set(LIBURING_INSTALL_DIR ${THIRD_PARTY_PATH}/install/liburing)
set(LIBURING_INCLUDE_DIR
    "${LIBURING_INSTALL_DIR}/include"
    CACHE PATH "liburing include directory." FORCE)
set(LIBURING_LIBRARIES
    "${LIBURING_INSTALL_DIR}/lib/liburing.a"
    CACHE FILEPATH "liburing library." FORCE)

ExternalProject_Add(
  extern_liburing
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${LIBURING_SOURCES_DIR}
  BINARY_DIR ${LIBURING_SOURCES_DIR}
  PREFIX ${LIBURING_BINARY_DIR}
  CONFIGURE_COMMAND ./configure --prefix=${LIBURING_INSTALL_DIR}
  BUILD_COMMAND $(MAKE) -j$(nproc)
  INSTALL_COMMAND $(MAKE) install
    COMMAND rm -rf ${LIBURING_SOURCES_DIR}/tmp
    COMMAND rm -rf ${LIBURING_SOURCES_DIR}/src/extern_liburing-stamp
  )

add_library(liburing STATIC IMPORTED GLOBAL)
set_property(TARGET liburing PROPERTY IMPORTED_LOCATION ${LIBURING_LIBRARIES})
add_dependencies(liburing extern_liburing)
