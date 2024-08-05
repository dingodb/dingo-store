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
message(STATUS "Include libunwind...")

set(LIBUNWIND_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/libunwind)
set(LIBUNWIND_BINARY_DIR ${THIRD_PARTY_PATH}/build/libunwind)
set(LIBUNWIND_INSTALL_DIR ${THIRD_PARTY_PATH}/install/libunwind)
set(LIBUNWIND_INCLUDE_DIR
    "${LIBUNWIND_INSTALL_DIR}/include"
    CACHE PATH "libunwind include directory." FORCE)
set(LIBUNWIND_LIBRARIES
    "${LIBUNWIND_INSTALL_DIR}/lib/libunwind.a"
    CACHE FILEPATH "libunwind library." FORCE)
set(LIBUNWIND_GENERIC_LIBRARIES
    "${LIBUNWIND_INSTALL_DIR}/lib/libunwind-generic.a"
    CACHE FILEPATH "libunwind generic library." FORCE)
set(LIBUNWIND_ARCH_LIBRARIES
    "${LIBUNWIND_INSTALL_DIR}/lib/libunwind-${CMAKE_HOST_SYSTEM_PROCESSOR}.a"
    CACHE FILEPATH "libunwind library." FORCE)

ExternalProject_Add(
  extern_libunwind
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${LIBUNWIND_SOURCES_DIR}
  BINARY_DIR ${LIBUNWIND_BINARY_DIR}
  PREFIX ${LIBUNWIND_BINARY_DIR}
  CONFIGURE_COMMAND autoreconf -i ${LIBUNWIND_SOURCES_DIR}
  COMMAND ${LIBUNWIND_SOURCES_DIR}/configure --prefix ${LIBUNWIND_INSTALL_DIR} --disable-minidebuginfo --enable-shared
          --enable-static --with-pic --disable-msabi-support
  BUILD_COMMAND $(MAKE)
  INSTALL_COMMAND $(MAKE) install)

add_library(libunwind STATIC IMPORTED GLOBAL)
set_property(TARGET libunwind PROPERTY IMPORTED_LOCATION ${LIBUNWIND_LIBRARIES} ${LIBUNWIND_GENERIC_LIBRARIES}
                                       ${LIBUNWIND_ARCH_LIBRARIES})
add_dependencies(libunwind extern_libunwind)
