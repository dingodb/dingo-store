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
message(STATUS "Include hdf5...")

SET(HDF5_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/hdf5)
SET(HDF5_BINARY_DIR ${THIRD_PARTY_PATH}/build/hdf5)
SET(HDF5_INSTALL_DIR ${THIRD_PARTY_PATH}/install/hdf5)
SET(HDF5_INCLUDE_DIR "${HDF5_INSTALL_DIR}/include" CACHE PATH "hdf5 include directory." FORCE)

IF(THIRD_PARTY_BUILD_TYPE MATCHES "Debug")
    SET(HDF5_LIBRARIES "${HDF5_INSTALL_DIR}/lib/libhdf5_debug.a" CACHE FILEPATH "hdf5 library." FORCE)
    SET(HDF5_CPP_LIBRARIES "${HDF5_INSTALL_DIR}/lib/libhdf5_cpp_debug.a" CACHE FILEPATH "hdf5 library." FORCE)
    SET(HDF5_HL_CPP_LIBRARIES "${HDF5_INSTALL_DIR}/lib/libhdf5_hl_cpp_debug.a" CACHE FILEPATH "hdf5 library." FORCE)
ELSE()
    SET(HDF5_LIBRARIES "${HDF5_INSTALL_DIR}/lib/libhdf5.a" CACHE FILEPATH "hdf5 library." FORCE)
    SET(HDF5_CPP_LIBRARIES "${HDF5_INSTALL_DIR}/lib/libhdf5_cpp.a" CACHE FILEPATH "hdf5 library." FORCE)
    SET(HDF5_HL_CPP_LIBRARIES "${HDF5_INSTALL_DIR}/lib/libhdf5_hl_cpp.a" CACHE FILEPATH "hdf5 library." FORCE)
ENDIF()

ExternalProject_Add(
    extern_hdf5
    ${EXTERNAL_PROJECT_LOG_ARGS}

    DEPENDS zlib

    SOURCE_DIR ${HDF5_SOURCES_DIR}
    BINARY_DIR ${HDF5_BINARY_DIR}
    PREFIX ${HDF5_BINARY_DIR}

    CMAKE_ARGS
    -C ${HDF5_SOURCES_DIR}/config/cmake/cacheinit.cmake
    -G "Unix Makefiles"
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DHDF5_ENABLE_SZIP_SUPPORT=OFF
    -DHDF5_ENABLE_Z_LIB_SUPPORT=ON
    -DHDF5_BUILD_CPP_LIB=ON
    -DHDF5_BUILD_EXAMPLES=OFF
    -DHDF5_BUILD_TOOLS=OFF
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_TESTING=OFF
    -DHDF5_BUILD_JAVA=OFF
    -DHDF5_BUILD_FORTRAN=OFF
    -DHDF5_BUILD_HL_GIF_TOOLS=OFF
    -DCMAKE_INSTALL_PREFIX=${HDF5_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
)

ADD_LIBRARY(hdf5 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET hdf5 PROPERTY IMPORTED_LOCATION ${HDF5_LIBRARIES})
ADD_DEPENDENCIES(hdf5 extern_hdf5)

ADD_LIBRARY(hdf5_cpp STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET hdf5_cpp PROPERTY IMPORTED_LOCATION ${HDF5_CPP_LIBRARIES})
ADD_DEPENDENCIES(hdf5_cpp extern_hdf5)

