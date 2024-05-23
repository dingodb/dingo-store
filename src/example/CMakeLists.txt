add_executable(sdk_rawkv_example
    sdk_rawkv_example.cc)
target_link_libraries(sdk_rawkv_example
    sdk
)

add_executable(sdk_transaction_example
    sdk_transaction_example.cc)
target_link_libraries(sdk_transaction_example
    sdk
)

add_executable(sdk_transaction_gc_example
    sdk_transaction_gc_example.cc)
target_link_libraries(sdk_transaction_gc_example
    sdk
)

add_executable(sdk_transaction_gc_op_all_and_lock_example
    sdk_transaction_gc_op_all_and_lock_example.cc)
target_link_libraries(sdk_transaction_gc_op_all_and_lock_example
    sdk
)

add_executable(sdk_vector_example
    sdk_vector_example.cc)
target_link_libraries(sdk_vector_example
    sdk
)

add_executable(txn_scan_test
    txn_scan_test.cc)
target_link_libraries(txn_scan_test
    sdk
)
