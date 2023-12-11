enable_testing()

SET(INTEGRATION_TEST_BIN "dingodb_integration_test")

set(INTEGRATION_TEST_COMMON_SRCS
  ${CMAKE_SOURCE_DIR}/src/common/uuid.cc
)

file(GLOB INTEGRATION_TEST_SRCS "test_*.cc")
file(GLOB INTEGRATION_TEST_REPORT_SRCS report/*.cc)

include_directories(${CMAKE_SOURCE_DIR}/test/integration_test)

add_executable(${INTEGRATION_TEST_BIN}
                main.cc
                ${INTEGRATION_TEST_COMMON_SRCS}
                ${INTEGRATION_TEST_REPORT_SRCS}
                ${INTEGRATION_TEST_SRCS}
                )

add_dependencies(${INTEGRATION_TEST_BIN} sdk fmt gtest glog)

target_link_libraries(${INTEGRATION_TEST_BIN}
                      PRIVATE
                      $<TARGET_OBJECTS:PROTO_OBJS>
                      sdk
                      ${PROTOBUF_LIBRARY}
                      ${GFLAGS_LIBRARIES}
                      ${FMT_LIBRARIES}
                      ${GLOG_LIBRARIES}
                      ${GTEST_LIBRARIES}
                      ${BRPC_LIBRARIES}
                      )