
find_package(SWIG 4.2 REQUIRED)
if(SWIG_FOUND)
  message("SWIG found: ${SWIG_EXECUTABLE}, dir: ${SWIG_DIR}, version: ${SWIG_VERSION}")
endif()

# Find Python 3
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

include(UseSWIG)

set(CMAKE_SWIG_FLAGS)
set(SWIG_SOURCE_FILE_EXTENSIONS swg)

list(APPEND CMAKE_SWIG_FLAGS "-DSWIGWORDSIZE64" "-c++")

set_property(SOURCE dingosdk.swg PROPERTY CPLUSPLUS ON)

swig_add_library(dingosdk 
  TYPE SHARED 
  LANGUAGE python 
  SOURCES dingosdk.swg
)

target_include_directories(dingosdk 
  PRIVATE 
  ${Python3_INCLUDE_DIRS}
)

target_link_libraries(dingosdk
  PRIVATE
  sdk
)

configure_file(pysdk_rawkv_example.py pysdk_rawkv_example.py COPYONLY)
configure_file(pysdk_vector_example.py pysdk_vector_example.py COPYONLY)