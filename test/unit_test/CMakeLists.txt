enable_testing()

add_compile_definitions(GLOG_CUSTOM_PREFIX_SUPPORT=ON)

add_subdirectory(common)
add_subdirectory(coprocessor)
add_subdirectory(engine)
add_subdirectory(mvcc)
add_subdirectory(vector)
add_subdirectory(misc)
add_subdirectory(document)
add_subdirectory(txn)
add_subdirectory(br)


add_executable(dingodb_unit_test main.cc)

add_dependencies(dingodb_unit_test ${DEPEND_LIBS})



set(UNIT_TEST_LIBS
    $<TARGET_OBJECTS:PROTO_OBJS>
    $<TARGET_OBJECTS:DINGODB_OBJS>
    ${DYNAMIC_LIB}
    ${VECTOR_LIB}
    serial
    $<TARGET_OBJECTS:UNIT_TEST_COMMON_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_COPROCESSOR_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_ENGINE_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_MVCC_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_VECTOR_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_MISC_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_DOCUMENT_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_TXN_OBJS>
    $<TARGET_OBJECTS:UNIT_TEST_BR_OBJS>)

set(UNIT_TEST_LIBS ${UNIT_TEST_LIBS} ${GTEST_LIBRARIES} ${GMOCK_LIBRARIES} "-Xlinker \"-(\"" ${BLAS_LIBRARIES}
                   "-Xlinker \"-)\"")

target_link_libraries(dingodb_unit_test ${UNIT_TEST_LIBS})
