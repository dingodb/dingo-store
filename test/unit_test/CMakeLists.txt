enable_testing()

add_subdirectory(sdk)

SET(UNIT_TEST_BIN "dingodb_unit_test")

file(GLOB TEST_SRCS "test_*.cc")

add_executable(${UNIT_TEST_BIN}
                main.cc
                ${TEST_SRCS}
              )

add_dependencies(${UNIT_TEST_BIN} ${DEPEND_LIBS} sdk)

target_link_libraries(${UNIT_TEST_BIN}
                      $<TARGET_OBJECTS:PROTO_OBJS>
                      $<TARGET_OBJECTS:DINGODB_OBJS>
                      ${DYNAMIC_LIB}
                      ${VECTOR_LIB}
                      sdk
                      $<TARGET_OBJECTS:SDK_UNIT_TEST_OBJS>
                      ${GTEST_LIBRARIES}
                      ${GMOCK_LIBRARIES}
                      "-Xlinker \"-(\""
                      ${BLAS_LIBRARIES}
                      "-Xlinker \"-)\""
                      )

