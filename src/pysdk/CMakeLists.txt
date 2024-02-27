
find_package(SWIG REQUIRED)
if(SWIG_FOUND)
  message("SWIG found: ${SWIG_EXECUTABLE}, dir: ${SWIG_DIR}, version: ${SWIG_VERSION}")
endif()

# Find Python 3
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

include(UseSWIG)

set(CMAKE_SWIG_FLAGS)
set(SWIG_SOURCE_FILE_EXTENSIONS swig)

list(APPEND CMAKE_SWIG_FLAGS "-DSWIGWORDSIZE64" "-c++" "-py3")

set_property(SOURCE dingosdk.swig PROPERTY CPLUSPLUS ON)

swig_add_library(dingosdk 
  TYPE SHARED 
  LANGUAGE python 
  SOURCES dingosdk.swig
)

set_target_properties(dingosdk 
  PROPERTIES
  SWIG_USE_TARGET_INCLUDE_DIRECTORIES ON
)

target_include_directories(dingosdk 
  PRIVATE 
  ${Python3_INCLUDE_DIRS}
)

target_link_libraries(dingosdk
  PRIVATE
  sdk
)
