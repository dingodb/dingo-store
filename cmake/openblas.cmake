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
message(STATUS "Include openblas...")

SET(OPENBLAS_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/openblas)
SET(OPENBLAS_BINARY_DIR ${THIRD_PARTY_PATH}/build/openblas)
SET(OPENBLAS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/openblas)
SET(OPENBLAS_INCLUDE_DIR "${OPENBLAS_INSTALL_DIR}/include" CACHE PATH "openblas include directory." FORCE)
SET(OPENBLAS_LIBRARIES "${OPENBLAS_INSTALL_DIR}/lib/libopenblas.a" CACHE FILEPATH "openblas library." FORCE)

ExternalProject_Add(
    extern_openblas
    ${EXTERNAL_PROJECT_LOG_ARGS}

    SOURCE_DIR ${OPENBLAS_SOURCES_DIR}
    BINARY_DIR ${OPENBLAS_BINARY_DIR}
    PREFIX ${OPENBLAS_BINARY_DIR}

    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${OPENBLAS_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${OPENBLAS_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${prefix_path}
    -DBUILD_WITHOUT_LAPACK=OFF
    -DBUILD_WITHOUT_CBLAS=ON
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_TESTING=OFF
    -DC_LAPACK=ON
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${OPENBLAS_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${OPENBLAS_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
    BUILD_COMMAND $(MAKE) NO_LAPACK=1 NO_AFFINITY=1 USE_OPENMP=1 USE_SIMPLE_THREADED_LEVEL3=1 NO_WARMUP=1
    INSTALL_COMMAND $(MAKE) install
)

ADD_LIBRARY(openblas STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET openblas PROPERTY IMPORTED_LOCATION ${OPENBLAS_LIBRARIES})
ADD_DEPENDENCIES(openblas extern_openblas)
