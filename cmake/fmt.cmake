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
message(STATUS "Include fmt...")

SET(FMT_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/fmt)
SET(FMT_BINARY_DIR ${THIRD_PARTY_PATH}/build/fmt)
SET(FMT_INSTALL_DIR ${THIRD_PARTY_PATH}/install/fmt)
SET(FMT_INCLUDE_DIR "${FMT_INSTALL_DIR}/include" CACHE PATH "fmt include directory." FORCE)
SET(FMT_LIBRARIES "${FMT_INSTALL_DIR}/lib/libfmt.a" CACHE FILEPATH "fmt library." FORCE)

ExternalProject_Add(
    extern_fmt
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${FMT_SOURCES_DIR}
    BINARY_DIR ${FMT_BINARY_DIR}
    PREFIX ${FMT_BINARY_DIR}

    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${FMT_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${FMT_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    -DFMT_TEST=OFF
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${FMT_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${FMT_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
    BUILD_COMMAND $(MAKE)
    INSTALL_COMMAND mkdir -p ${FMT_INSTALL_DIR}/lib/ COMMAND cp ${FMT_BINARY_DIR}/libfmt${CMAKE_STATIC_LIBRARY_SUFFIX} ${FMT_LIBRARIES} COMMAND cp -r ${FMT_SOURCES_DIR}/include ${FMT_INCLUDE_DIR}/
)

ADD_LIBRARY(fmt STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET fmt PROPERTY IMPORTED_LOCATION ${FMT_LIBRARIES})
ADD_DEPENDENCIES(fmt extern_fmt)
