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

SET(FAISS_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/faiss)
SET(FAISS_BINARY_DIR ${THIRD_PARTY_PATH}/build/faiss)
SET(FAISS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/faiss)
SET(FAISS_INCLUDE_DIR "${FAISS_INSTALL_DIR}/include" CACHE PATH "faiss include directory." FORCE)
SET(FAISS_LIBRARIES "${FAISS_INSTALL_DIR}/lib/libfaiss.a" CACHE FILEPATH "faiss library." FORCE)

set(prefix_path "${THIRD_PARTY_PATH}/install/openblas")

ExternalProject_Add(
        extern_faiss
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS openblas
        SOURCE_DIR ${FAISS_SOURCES_DIR}
        BINARY_DIR ${FAISS_BINARY_DIR}
        PREFIX ${FAISS_INSTALL_DIR}
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${FAISS_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${FAISS_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DFAISS_ENABLE_GPU=OFF
        -DFAISS_ENABLE_PYTHON=OFF
        -DBLA_STATIC=ON
        -DBUILD_TESTING=OFF
        -DBUILD_SHARED_LIBS=OFF
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${FAISS_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR:PATH=${FAISS_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
        BUILD_COMMAND $(MAKE)
        INSTALL_COMMAND $(MAKE) install
#        INSTALL_COMMAND mkdir -p ${FAISS_INSTALL_DIR}/lib/ COMMAND cp ${FAISS_BINARY_DIR}/lib/libfaiss.a ${FAISS_LIBRARIES} COMMAND cp -r ${FAISS_BINARY_DIR}/include ${FAISS_INCLUDE_DIR}/
)

ADD_DEPENDENCIES(extern_faiss openblas)
ADD_LIBRARY(faiss STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET faiss PROPERTY IMPORTED_LOCATION ${FAISS_LIBRARIES})
ADD_DEPENDENCIES(faiss extern_faiss)
