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
message(STATUS "Include tantivy-search...")

set(TANTIVY_SEARCH_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/tantivy-search)
set(TANTIVY_SEARCH_BINARY_DIR ${THIRD_PARTY_PATH}/build/tantivy-search)
set(TANTIVY_SEARCH_INSTALL_DIR ${THIRD_PARTY_PATH}/install/tantivy-search)
set(TANTIVY_SEARCH_INCLUDE_DIR
    "${TANTIVY_SEARCH_INSTALL_DIR}/include"
    CACHE PATH "tantivy-search include directory." FORCE)
set(TANTIVY_SEARCH_LIBRARIES
    "${TANTIVY_SEARCH_INSTALL_DIR}/lib/libtantivy_search.a"
    CACHE FILEPATH "tantivy-search library." FORCE)

ExternalProject_Add(
  extern_tantivy-search
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${TANTIVY_SEARCH_SOURCES_DIR}
  BINARY_DIR ${TANTIVY_SEARCH_BINARY_DIR}
  PREFIX ${TANTIVY_SEARCH_BINARY_DIR}
  CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
             -DCMAKE_INSTALL_PREFIX=${TANTIVY_SEARCH_INSTALL_DIR}
             -DCMAKE_INSTALL_LIBDIR=${TANTIVY_SEARCH_INSTALL_DIR}/lib
             -DCMAKE_POSITION_INDEPENDENT_CODE=ON
             -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
             -DCMAKE_PREFIX_PATH=${prefix_path}
             -DBUILD_TESTS=OFF
             -DBUILD_BENCHMARK=OFF
             -DBUILD_EXAMPLES=OFF
             ${EXTERNAL_OPTIONAL_ARGS}
  LIST_SEPARATOR |
  CMAKE_CACHE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${TANTIVY_SEARCH_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${TANTIVY_SEARCH_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
  BUILD_COMMAND $(MAKE)
  INSTALL_COMMAND mkdir -p ${TANTIVY_SEARCH_INSTALL_DIR}/lib/
  COMMAND cp ${TANTIVY_SEARCH_BINARY_DIR}/libtantivy_search.a
          ${TANTIVY_SEARCH_LIBRARIES}
  COMMAND cp -r ${TANTIVY_SEARCH_SOURCES_DIR}/include
          ${TANTIVY_SEARCH_INCLUDE_DIR}/)

add_library(tantivy-search STATIC IMPORTED GLOBAL)
set_property(TARGET tantivy-search PROPERTY IMPORTED_LOCATION
                                            ${TANTIVY_SEARCH_LIBRARIES})
add_dependencies(tantivy-search extern_tantivy-search)
