


set(TEST_DEP_SRCS ${CONFIG_SRCS} ${CRONTAB_SRCS} ${PROTO_SRCS})

enable_testing()

file(GLOB TEST_SRCS "test_*.cc")
foreach(TEST_SRC ${TEST_SRCS})
  message(STATUS "TEST_SRC: ${TEST_SRC}")
  get_filename_component(TEST_WE ${TEST_SRC} NAME_WE)
  add_executable(${TEST_WE}
                 ${TEST_SRC}
                 ${TEST_DEP_SRCS})
  add_dependencies(${TEST_WE} ${DEPEND_LIBS})
  target_link_libraries(${TEST_WE}
                        "-Xlinker \"-(\""
                        ${GTEST_MAIN_LIBRARIES}
                        ${GTEST_LIBRARIES}
                        ${DYNAMIC_LIB}
                        "-Xlinker \"-)\""
                        )
endforeach()
