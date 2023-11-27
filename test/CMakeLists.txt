enable_testing()

SET(UNIT_TEST_BIN "dingodb_unit_test")

file(GLOB TEST_SRCS "test_*.cc")

add_executable(${UNIT_TEST_BIN}
                main.cc
                ${TEST_SRCS}
                $<TARGET_OBJECTS:DINGODB_OBJS>
                $<TARGET_OBJECTS:PROTO_OBJS>
              )

add_dependencies(${UNIT_TEST_BIN} ${DEPEND_LIBS})

target_link_libraries(${UNIT_TEST_BIN}
                      "-Xlinker \"-(\""
                      ${GTEST_LIBRARIES}
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\""
                      )

add_subdirectory(sdk)