# Find the LevelDB library
# LEVELDB_INCLUDE_DIRS - where to find leveldb/db.h, etc.
# LEVELDB_LIBRARIES - List of libraries when using LevelDB.
# LEVELDB_FOUND - True if LevelDB found.

find_path(LEVELDB_INCLUDE_DIRS
  NAMES leveldb/db.h
  HINTS ${leveldb_ROOT_DIR}/include)

find_library(LEVELDB_LIBRARIES
    NAMES leveldb
    HINTS ${leveldb_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(leveldb DEFAULT_MSG LEVELDB_LIBRARIES LEVELDB_INCLUDE_DIRS)

mark_as_advanced(
  LEVELDB_LIBRARIES
  LEVELDB_INCLUDE_DIRS)

if(LEVELDB_FOUND AND NOT (TARGET leveldb::leveldb))
    add_library (leveldb::leveldb UNKNOWN IMPORTED)
    set_target_properties(leveldb::leveldb
        PROPERTIES
        IMPORTED_LOCATION ${LEVELDB_LIBRARIES}
        INTERFACE_INCLUDE_DIRECTORIES ${LEVELDB_INCLUDE_DIRS})
endif() 
