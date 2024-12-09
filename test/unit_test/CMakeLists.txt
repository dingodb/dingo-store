enable_testing()

add_subdirectory(common)
add_subdirectory(coprocessor)
add_subdirectory(engine)
add_subdirectory(mvcc)
add_subdirectory(vector)
add_subdirectory(misc)
add_subdirectory(document)
add_subdirectory(txn)

set(UNIT_TEST_BIN "dingodb_unit_test")

add_executable(${UNIT_TEST_BIN} main.cc)

add_dependencies(${UNIT_TEST_BIN} ${DEPEND_LIBS})

set(UNIT_TEST_LIBS
    $<TARGET_OBJECTS:PROTO_OBJS>
    $<TARGET_OBJECTS:DINGODB_OBJS>
    ${DYNAMIC_LIB}
    ${VECTOR_LIB}
    $<TARGET_OBJECTS:UNIT_TEST_COMMON_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_COPROCESSOR_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_ENGINE_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_MVCC_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_VECTOR_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_MISC_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_DOCUMENT_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_TXN_OBJS>)

set(UNIT_TEST_LIBS ${UNIT_TEST_LIBS} ${GTEST_LIBRARIES} ${GMOCK_LIBRARIES} "-Xlinker \"-(\"" ${BLAS_LIBRARIES}
                   "-Xlinker \"-)\"")

target_link_libraries(${UNIT_TEST_BIN} ${UNIT_TEST_LIBS})
