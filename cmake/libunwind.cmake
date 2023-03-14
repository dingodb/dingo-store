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

SET(LIBUNWIND_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/libunwind)
SET(LIBUNWIND_BINARY_DIR ${THIRD_PARTY_PATH}/build/libunwind)
SET(LIBUNWIND_INSTALL_DIR ${THIRD_PARTY_PATH}/install/libunwind)
SET(LIBUNWIND_INCLUDE_DIR "${LIBUNWIND_INSTALL_DIR}/include" CACHE PATH "libunwind include directory." FORCE)
SET(LIBUNWIND_LIBRARIES "${LIBUNWIND_INSTALL_DIR}/lib/libunwind.a" CACHE FILEPATH "libunwind library." FORCE)
SET(LIBUNWIND_GENERIC_LIBRARIES "${LIBUNWIND_INSTALL_DIR}/lib/libunwind-generic.a" CACHE FILEPATH "libunwind generic library." FORCE)
SET(LIBUNWIND_ARCH_LIBRARIES "${LIBUNWIND_INSTALL_DIR}/lib/libunwind-x86_64.a" CACHE FILEPATH "libunwind x86_64 library." FORCE)

ExternalProject_Add(
        extern_libunwind
        ${EXTERNAL_PROJECT_LOG_ARGS}
        SOURCE_DIR ${LIBUNWIND_SOURCES_DIR}
        BINARY_DIR ${LIBUNWIND_BINARY_DIR}
        PREFIX ${LIBUNWIND_INSTALL_DIR}
        CONFIGURE_COMMAND ${LIBUNWIND_SOURCES_DIR}/configure --prefix ${LIBUNWIND_INSTALL_DIR} --disable-minidebuginfo
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install
)

ADD_LIBRARY(libunwind STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET libunwind PROPERTY IMPORTED_LOCATION ${LIBUNWIND_LIBRARIES} ${LIBUNWIND_GENERIC_LIBRARIES} ${LIBUNWIND_ARCH_LIBRARIES})
ADD_DEPENDENCIES(libunwind extern_libunwind)
