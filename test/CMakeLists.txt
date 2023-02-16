

# add_library(SOURCES_DEBUG_LIB OBJECT ${CONFIG_SRCS} )

enable_testing()
include(GoogleTest)
find_package(GTest 1.13.0)

file(GLOB TEST_SRCS "test_*.cc")
foreach(TEST_SRC ${TEST_SRCS})
  message(STATUS "TEST_SRC: ${TEST_SRC}")
  get_filename_component(TEST_WE ${TEST_SRC} NAME_WE)
  add_executable(${TEST_WE}
                 ${TEST_SRC}
                 ${CONFIG_SRCS})
  add_dependencies(${TEST_WE} ${DEPEND_LIBS})
  target_link_libraries(${TEST_WE}
                        "-Xlinker \"-(\""
                        ${GTEST_MAIN_LIBRARIES}
                        ${GTEST_LIBRARIES}
                        ${DYNAMIC_LIB}
                        "-Xlinker \"-)\""
                        )
  gtest_discover_tests(${TEST_WE})
endforeach()
