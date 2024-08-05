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
message(STATUS "Include ftxui...")

set(FTXUI_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/FTXUI)
set(FTXUI_BINARY_DIR ${THIRD_PARTY_PATH}/build/ftxui)
set(FTXUI_INSTALL_DIR ${THIRD_PARTY_PATH}/install/ftxui)
set(FTXUI_INCLUDE_DIR
    "${FTXUI_INSTALL_DIR}/include"
    CACHE PATH "ftxui include directory." FORCE)

set(FTXUI_COMPONENT_LIBRARY
    "${FTXUI_INSTALL_DIR}/lib/libftxui-component.a"
    CACHE FILEPATH "ftxui library." FORCE)
set(FTXUI_DOM_LIBRARY
    "${FTXUI_INSTALL_DIR}/lib/libftxui-dom.a"
    CACHE FILEPATH "ftxui library." FORCE)
set(FTXUI_SCREEN_LIBRARY
    "${FTXUI_INSTALL_DIR}/lib/libftxui-screen.a"
    CACHE FILEPATH "ftxui library." FORCE)

ExternalProject_Add(
  extern_ftxui
  ${EXTERNAL_PROJECT_LOG_ARGS}
  SOURCE_DIR ${FTXUI_SOURCES_DIR}
  BINARY_DIR ${FTXUI_BINARY_DIR}
  PREFIX ${FTXUI_BINARY_DIR}
  CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
             -DCMAKE_INSTALL_PREFIX=${FTXUI_INSTALL_DIR}
             -DCMAKE_INSTALL_LIBDIR=${FTXUI_INSTALL_DIR}/lib
             -DCMAKE_POSITION_INDEPENDENT_CODE=ON
             -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
             ${EXTERNAL_OPTIONAL_ARGS}
  LIST_SEPARATOR |
  CMAKE_CACHE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${FTXUI_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${FTXUI_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE})

add_library(ftxui STATIC IMPORTED GLOBAL)
set_property(TARGET ftxui PROPERTY IMPORTED_LOCATION ${FTXUI_COMPONENT_LIBRARY})
add_dependencies(ftxui extern_ftxui)
