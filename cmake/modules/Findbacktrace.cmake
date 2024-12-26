# Find the backtrace library
# BACKTRACE_FOUND - True if backtrace found.
# backtrace::backtrace - cmake target for libbacktrace

find_path(BACKTRACE_INCLUDE_DIRS
  NAMES backtrace.h
  HINTS ${backtrace_ROOT_DIR}/include)

find_library(BACKTRACE_LIBRARIES
    NAMES backtrace
    HINTS ${backtrace_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(backtrace DEFAULT_MSG BACKTRACE_LIBRARIES BACKTRACE_INCLUDE_DIRS)

mark_as_advanced(
  BACKTRACE_LIBRARIES
  BACKTRACE_INCLUDE_DIRS)

if(BACKTRACE_FOUND AND NOT (TARGET backtrace::backtrace))
    add_library (backtrace::backtrace UNKNOWN IMPORTED)
    set_target_properties(backtrace::backtrace  
        PROPERTIES
            IMPORTED_LOCATION ${BACKTRACE_LIBRARIES}
            INTERFACE_INCLUDE_DIRECTORIES ${BACKTRACE_INCLUDE_DIRS}
            INTERFACE_LINK_LIBRARIES dl)
endif()

