enable_testing()

add_subdirectory(legacy)
add_subdirectory(sdk)

if(WITH_DOC)
  add_subdirectory(document)
endif()

SET(UNIT_TEST_BIN "dingodb_unit_test")

add_executable(${UNIT_TEST_BIN} main.cc)

add_dependencies(${UNIT_TEST_BIN} ${DEPEND_LIBS} sdk)

set(UNIT_TEST_LIBS
  $<TARGET_OBJECTS:PROTO_OBJS>
  $<TARGET_OBJECTS:DINGODB_OBJS>
  ${DYNAMIC_LIB}
  ${VECTOR_LIB}
  $<TARGET_OBJECTS:LEGACY_UNIT_TEST_OBJS>
  sdk
  $<TARGET_OBJECTS:SDK_UNIT_TEST_OBJS>
)

if(WITH_DOC)
  list(APPEND UNIT_TEST_LIBS $<TARGET_OBJECTS:DOCUMENT_UNIT_TEST_OBJS>)
endif()

set(UNIT_TEST_LIBS
  ${UNIT_TEST_LIBS}
  ${GTEST_LIBRARIES}
  ${GMOCK_LIBRARIES}
  "-Xlinker \"-(\""
  ${BLAS_LIBRARIES}
  "-Xlinker \"-)\""
)

target_link_libraries(${UNIT_TEST_BIN} ${UNIT_TEST_LIBS})
