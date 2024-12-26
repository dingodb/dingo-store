# Find unwind
# This module defines
# UNWIND_FOUND, if false, do not try to link to unwind
# UNWIND_LIBRARIES, the libraries needed to use unwind
# UNWIND_INCLUDE_DIRS, where to find unwind.h

find_path(UNWIND_INCLUDE_DIRS
  NAMES unwind.h
  HINTS ${UNWIND_ROOT_DIR}/include)

find_library(UNWIND_LIBRARIES
    NAMES unwind
    HINTS ${UNWIND_ROOT_DIR}/lib)

find_library(UNWIND_GENERIC_LIBRARIES 
  NAMES unwind-generic 
  HINTS ${unwind_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(unwind DEFAULT_MSG UNWIND_LIBRARIES UNWIND_GENERIC_LIBRARIES UNWIND_INCLUDE_DIRS)

mark_as_advanced(
    UNWIND_LIBRARIES
    UNWIND_GENERIC_LIBRARIES
    UNWIND_INCLUDE_DIRS
)

if(UNWIND_FOUND AND NOT (TARGET unwind::unwind))
    add_library(unwind::unwind UNKNOWN IMPORTED)
    set_target_properties(unwind::unwind
        PROPERTIES
        IMPORTED_LOCATION ${UNWIND_LIBRARIES}
        INTERFACE_INCLUDE_DIRECTORIES ${UNWIND_INCLUDE_DIRS})

    add_library(unwind::unwind-generic UNKNOWN IMPORTED)
    set_target_properties(unwind::unwind-generic
        PROPERTIES
        IMPORTED_LOCATION ${UNWIND_GENERIC_LIBRARIES}
        INTERFACE_INCLUDE_DIRECTORIES ${UNWIND_INCLUDE_DIRS})
endif()
