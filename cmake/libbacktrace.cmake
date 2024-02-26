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
message(STATUS "Include libbacktrace...")

SET(LIBBACKTRACE_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/libbacktrace)
SET(LIBBACKTRACE_BINARY_DIR ${THIRD_PARTY_PATH}/build/libbacktrace)
SET(LIBBACKTRACE_INSTALL_DIR ${THIRD_PARTY_PATH}/install/libbacktrace)
SET(LIBBACKTRACE_INCLUDE_DIR "${LIBBACKTRACE_INSTALL_DIR}/include" CACHE PATH "libbacktrace include directory." FORCE)
SET(LIBBACKTRACE_LIBRARIES "${LIBBACKTRACE_INSTALL_DIR}/lib/libbacktrace.a" CACHE FILEPATH "libbacktrace library." FORCE)

ExternalProject_Add(
    extern_libbacktrace
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${LIBBACKTRACE_SOURCES_DIR}
    BINARY_DIR ${LIBBACKTRACE_BINARY_DIR}
    PREFIX ${LIBBACKTRACE_BINARY_DIR}

    CONFIGURE_COMMAND ${LIBBACKTRACE_SOURCES_DIR}/configure --prefix ${LIBBACKTRACE_INSTALL_DIR}
    BUILD_COMMAND $(MAKE) AM_CFLAGS=-fPIC
    INSTALL_COMMAND $(MAKE) install
)

ADD_LIBRARY(libbacktrace STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET libbacktrace PROPERTY IMPORTED_LOCATION ${LIBBACKTRACE_LIBRARIES})
ADD_DEPENDENCIES(libbacktrace extern_libbacktrace)
