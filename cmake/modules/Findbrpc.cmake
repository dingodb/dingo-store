# - Try to find brpc
# This module will also define the following variables:
#
# BRPC_INCLUDE_DIRS - where to find brpc headers
# BRPC_LIBRARIES - List of libraries when using brpc.
# BRPC_FOUND - True if zstd found.


find_path(BRPC_INCLUDE_DIRS
  NAMES brpc/server.h
  HINTS ${brpc_ROOT_DIR}/include)

find_library(BRPC_LIBRARIES
  NAMES brpc 
  HINTS ${brpc_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(brpc DEFAULT_MSG BRPC_LIBRARIES BRPC_INCLUDE_DIRS)

mark_as_advanced(
  BRPC_LIBRARIES
  BRPC_INCLUDE_DIRS)

if(BRPC_FOUND AND NOT (TARGET brpc::brpc))
  set(BRPC_DEPS_LIBS
      gflags
      OpenSSL::SSL
      leveldb::leveldb
      Snappy::snappy
      ZLIB::ZLIB
      fmt::fmt
      glog::glog
      protobuf::libprotobuf) 
  message("BRPC_DEPS_LIBS: ${BRPC_DEPS_LIBS}")
  add_library(brpc::brpc UNKNOWN IMPORTED)
  set_target_properties(brpc::brpc
    PROPERTIES
      IMPORTED_LOCATION ${BRPC_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${BRPC_INCLUDE_DIRS}
      INTERFACE_LINK_LIBRARIES "${BRPC_DEPS_LIBS}") 
endif()

