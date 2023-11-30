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
