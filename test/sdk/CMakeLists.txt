include_directories(${CMAKE_SOURCE_DIR}/src/coordinator)
include_directories(${CMAKE_SOURCE_DIR}/src/client)
include_directories(${CMAKE_SOURCE_DIR}/src/sdk)
# use for gmock
include_directories(${GTEST_INCLUDE_DIR}/include)

include(GoogleTest)

file(GLOB TEST_SRCS "test_*.cc")
foreach(TEST_SRC ${TEST_SRCS})
  message(STATUS "TEST_SRC: ${TEST_SRC}")
  get_filename_component(TEST_WE ${TEST_SRC} NAME_WE)

  add_executable(${TEST_WE}
    ${TEST_SRC}
  )

  target_link_libraries(${TEST_WE}
    sdk
    ${GTEST_MAIN_LIBRARIES}
    ${GMOCK_LIBRARIES}
    ${GMOCK_MAIN_LIBRARIES}
  )

  gtest_discover_tests(${TEST_WE})
endforeach()