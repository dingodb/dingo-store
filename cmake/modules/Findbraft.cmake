# Find braft
# Find the braft library and includes
# BRAFT_FOUND - True if braft found.
# braft::braft - cmake target for libbraft

find_path(BRAFT_INCLUDE_DIRS
  NAMES braft/raft.h
  HINTS ${braft_ROOT_DIR}/include)

find_library(BRAFT_LIBRARIES
  NAMES braft
  HINTS ${braft_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(braft DEFAULT_MSG BRAFT_LIBRARIES BRAFT_INCLUDE_DIRS)

mark_as_advanced(
  BRAFT_LIBRARIES
  BRAFT_INCLUDE_DIRS)

if(BRAFT_FOUND AND NOT (TARGET braft::braft))
  find_package(brpc REQUIRED)
  add_library (braft::braft UNKNOWN IMPORTED)
  set_target_properties(braft::braft
    PROPERTIES
      IMPORTED_LOCATION ${BRAFT_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${BRAFT_INCLUDE_DIRS}
      INTERFACE_LINK_LIBRARIES brpc::brpc)  
endif()



