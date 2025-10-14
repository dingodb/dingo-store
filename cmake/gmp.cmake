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
message(STATUS "Include gmp...")

set(GMP_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/gmp/gmp-6.3.0)
set(GMP_BINARY_DIR ${THIRD_PARTY_PATH}/build/gmp)
set(GMP_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gmp)
set(GMP_INCLUDE_DIR
	"${GMP_INSTALL_DIR}/include"
	CACHE PATH "gmp include directory." FORCE)
set(GMP_LIBRARIES
	"${GMP_INSTALL_DIR}/lib/libgmp.a"
	CACHE FILEPATH "gmp library." FORCE)
set(GMPXX_LIBRARIES
        "${GMP_INSTALL_DIR}/lib/libgmpxx.a"
        CACHE FILEPATH "gmpxx library." FORCE)

if(THIRD_PARTY_BUILD_TYPE MATCHES "Debug")
	set(GMP_DEBUG_FLAG "--enable-debug")
elseif(THIRD_PARTY_BUILD_TYPE MATCHES "RelWithDebInfo")
	set(GMP_DEBUG_FLAG "--enable-debug")
endif()

ExternalProject_Add(
  extern_gmp
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${GMP_SOURCES_DIR}
  BINARY_DIR ${GMP_BINARY_DIR}
  PREFIX ${GMP_BINARY_DIR}
  CONFIGURE_COMMAND ${GMP_SOURCES_DIR}/configure --prefix ${GMP_INSTALL_DIR} --enable-cxx
  BUILD_COMMAND $(MAKE)
  INSTALL_COMMAND $(MAKE) install )

add_library(gmp STATIC IMPORTED GLOBAL)
set_property(TARGET gmp PROPERTY IMPORTED_LOCATION ${GMP_LIBRARIES})
add_dependencies(gmp extern_gmp)

add_library(gmpxx STATIC IMPORTED GLOBAL)
set_property(TARGET gmpxx PROPERTY IMPORTED_LOCATION ${GMPXX_LIBRARIES})
add_dependencies(gmpxx extern_gmp)

