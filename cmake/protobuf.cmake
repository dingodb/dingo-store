# Copyright (c) 2020-present dingodb.com, Inc. All Rights Reserved.
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
message(STATUS "Include protobuf...")

# Always invoke `FIND_PACKAGE(Protobuf)` for importing function
# protobuf_generate_cpp FIND_PACKAGE(Protobuf QUIET)
macro(UNSET_VAR VAR_NAME)
  unset(${VAR_NAME} CACHE)
  unset(${VAR_NAME})
endmacro()

unset_var(PROTOBUF_FOUND)
unset_var(PROTOBUF_PROTOC_EXECUTABLE)
unset_var(PROTOBUF_PROTOC_LIBRARY)
unset_var(PROTOBUF_LITE_LIBRARY)
unset_var(PROTOBUF_LIBRARY)
unset_var(PROTOBUF_INCLUDE_DIR)
unset_var(Protobuf_PROTOC_EXECUTABLE)

# Print and set the protobuf library information, finish this cmake process and
# exit from this file.
macro(PROMPT_PROTOBUF_LIB)
  set(protobuf_DEPS ${ARGN})

  message(STATUS "Protobuf protoc executable: ${PROTOBUF_PROTOC_EXECUTABLE}")
  message(STATUS "Protobuf-lite library: ${PROTOBUF_LITE_LIBRARY}")
  message(STATUS "Protobuf library: ${PROTOBUF_LIBRARY}")
  message(STATUS "Protoc library: ${PROTOBUF_PROTOC_LIBRARY}")
  message(STATUS "Protobuf version: ${PROTOBUF_VERSION}")
  include_directories(${PROTOBUF_INCLUDE_DIR})

  # Assuming that all the protobuf libraries are of the same type.
  if(${PROTOBUF_LIBRARY} MATCHES ${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(protobuf_LIBTYPE STATIC)
  elseif(${PROTOBUF_LIBRARY} MATCHES "${CMAKE_SHARED_LIBRARY_SUFFIX}$")
    set(protobuf_LIBTYPE SHARED)
  else()
    message(FATAL_ERROR "Unknown library type: ${PROTOBUF_LIBRARY}")
  endif()

  add_library(protobuf ${protobuf_LIBTYPE} IMPORTED GLOBAL)
  set_property(TARGET protobuf PROPERTY IMPORTED_LOCATION ${PROTOBUF_LIBRARY})

  add_library(protobuf_lite ${protobuf_LIBTYPE} IMPORTED GLOBAL)
  set_property(TARGET protobuf_lite PROPERTY IMPORTED_LOCATION
                                             ${PROTOBUF_LITE_LIBRARY})

  add_library(libprotoc ${protobuf_LIBTYPE} IMPORTED GLOBAL)
  set_property(TARGET libprotoc PROPERTY IMPORTED_LOCATION ${PROTOC_LIBRARY})

  add_executable(protoc IMPORTED GLOBAL)
  set_property(TARGET protoc PROPERTY IMPORTED_LOCATION
                                      ${PROTOBUF_PROTOC_EXECUTABLE})
  set(Protobuf_PROTOC_EXECUTABLE ${PROTOBUF_PROTOC_EXECUTABLE})

  foreach(dep ${protobuf_DEPS})
    add_dependencies(protobuf ${dep})
    add_dependencies(protobuf_lite ${dep})
    add_dependencies(libprotoc ${dep})
    add_dependencies(protoc ${dep})
  endforeach()

  return()
endmacro()
macro(SET_PROTOBUF_VERSION)
  exec_program(
    ${PROTOBUF_PROTOC_EXECUTABLE} ARGS
    --version
    OUTPUT_VARIABLE PROTOBUF_VERSION)
  string(REGEX MATCH "[0-9]+.[0-9]+" PROTOBUF_VERSION "${PROTOBUF_VERSION}")
endmacro()

function(build_protobuf TARGET_NAME)
  set(PROTOBUF_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/protobuf)
  set(PROTOBUF_BUILD_DIR ${THIRD_PARTY_PATH}/build/protobuf)
  set(PROTOBUF_INSTALL_DIR ${THIRD_PARTY_PATH}/install/protobuf)

  set(PROTOBUF_INCLUDE_DIR
      "${PROTOBUF_INSTALL_DIR}/include"
      CACHE PATH "protobuf include directory." FORCE)
  set(PROTOBUF_LITE_LIBRARY
      "${PROTOBUF_INSTALL_DIR}/lib/libprotobuf-lite${CMAKE_STATIC_LIBRARY_SUFFIX}"
      CACHE FILEPATH "protobuf lite library." FORCE)
  set(PROTOBUF_LIBRARY
      "${PROTOBUF_INSTALL_DIR}/lib/libprotobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
      CACHE FILEPATH "protobuf library." FORCE)
  set(PROTOBUF_LIBRARIES
      ${PROTOBUF_LIBRARY}
      CACHE FILEPATH "protobuf library." FORCE)
  set(PROTOBUF_PROTOC_LIBRARY
      "${PROTOBUF_INSTALL_DIR}/lib/libprotoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
      CACHE FILEPATH "protoc library." FORCE)
  set(PROTOBUF_PROTOC_EXECUTABLE
      "${PROTOBUF_INSTALL_DIR}/bin/protoc${CMAKE_EXECUTABLE_SUFFIX}"
      CACHE FILEPATH "protobuf executable." FORCE)

  ExternalProject_Add(
    ${TARGET_NAME}
    ${EXTERNAL_PROJECT_LOG_ARGS}
    DEPENDS zlib
    SOURCE_DIR ${PROTOBUF_SOURCES_DIR}
    BINARY_DIR ${PROTOBUF_BUILD_DIR}
    PREFIX ${PROTOBUF_BUILD_DIR}
    UPDATE_COMMAND ""
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
               -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
               -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
               -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
               -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
               -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
               -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DCMAKE_SKIP_RPATH=ON
               -Dprotobuf_WITH_ZLIB=ON
               -Dprotobuf_BUILD_TESTS=OFF
               -Dprotobuf_BUILD_SHARED_LIBS=OFF
               -DCMAKE_INSTALL_PREFIX=${PROTOBUF_INSTALL_DIR}
               -DCMAKE_INSTALL_LIBDIR=lib
               -DBUILD_SHARED_LIBS=OFF
               -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
               -DCMAKE_PREFIX_PATH=${ZLIB_INSTALL_DIR}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS
      -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
      -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF
      -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    INSTALL_COMMAND $(MAKE) install
    COMMAND mkdir -p ${PROTOBUF_INCLUDE_DIR}
    COMMAND cp -f ${ZLIB_INCLUDE_DIR}/zlib.h ${PROTOBUF_INCLUDE_DIR}/
    COMMAND cp -f ${ZLIB_INCLUDE_DIR}/zconf.h ${PROTOBUF_INCLUDE_DIR}/)

endfunction()

set(PROTOBUF_VERSION 3.21.12)

build_protobuf(extern_protobuf)
prompt_protobuf_lib(extern_protobuf zlib)
