enable_testing()

SET(INTEGRATION_TEST_BIN "dingodb_integration_test")

set(INTEGRATION_TEST_DEPEND_SRCS
  ${CMAKE_SOURCE_DIR}/src/common/uuid.cc
  ${REPORT_SRCS}
)

file(GLOB INTEGRATION_TEST_SRCS "*.cc")
list(REMOVE_ITEM INTEGRATION_TEST_SRCS "main.cc")

include_directories(${CMAKE_SOURCE_DIR}/test/integration_test)

add_executable(${INTEGRATION_TEST_BIN}
                main.cc
                ${INTEGRATION_TEST_DEPEND_SRCS}
                ${INTEGRATION_TEST_SRCS}
                )

add_dependencies(${INTEGRATION_TEST_BIN} sdk fmt gtest glog)

target_link_libraries(${INTEGRATION_TEST_BIN}
                      PRIVATE
                      $<TARGET_OBJECTS:PROTO_OBJS>
                      sdk
                      )