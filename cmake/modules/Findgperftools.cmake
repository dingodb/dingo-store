# Find gperftools
# This module defines
# GPERFTOOLS_FOUND, if false, do not try to link to gperftools
# GPERFTOOLS_LIBRARIES, the libraries needed to use gperftools
# GPERFTOOLS_MINIMAL_LIBRARIES, the minimal libraries needed to use gperftools
# GPERFTOOLS_INCLUDE_DIRS, where to find gperftools.h

find_path(GPERFTOOLS_INCLUDE_DIRS
  NAMES gperftools/profiler.h
  HINTS ${GPERFTOOLS_ROOT_DIR}/include)

find_library(GPERFTOOLS_LIBRARIES
    NAMES tcmalloc_and_profiler
    HINTS ${GPERFTOOLS_ROOT_DIR}/lib)

find_library(GPERFTOOLS_MINIMAL_LIBRARIES
    NAMES tcmalloc_minimal
    HINTS ${GPERFTOOLS_ROOT_DIR}/lib)


include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(gperftools DEFAULT_MSG GPERFTOOLS_LIBRARIES GPERFTOOLS_MINIMAL_LIBRARIES GPERFTOOLS_INCLUDE_DIRS)

mark_as_advanced(
    GPERFTOOLS_LIBRARIES
    GPERFTOOLS_MINIMAL_LIBRARIES
    GPERFTOOLS_INCLUDE_DIRS
)

if(GPERFTOOLS_FOUND AND NOT (TARGET gperftools::gperftools))
    add_library (gperftools::gperftools UNKNOWN IMPORTED)
    set_target_properties(gperftools::gperftools
        PROPERTIES
        IMPORTED_LOCATION ${GPERFTOOLS_LIBRARIES}
        INTERFACE_INCLUDE_DIRECTORIES ${GPERFTOOLS_INCLUDE_DIRS})

    add_library (gperftools::gperftools_minimal UNKNOWN IMPORTED)
    set_target_properties(gperftools::gperftools_minimal
        PROPERTIES
        IMPORTED_LOCATION ${GPERFTOOLS_MINIMAL_LIBRARIES}
        INTERFACE_INCLUDE_DIRECTORIES ${GPERFTOOLS_INCLUDE_DIRS})
endif()

