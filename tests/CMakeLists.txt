set(ZRPC_TESTS
    test_zrpc
    test_zrpc_client
    test_zrpc_server
    test_zrpc_pub
    test_zrpc_sub
)

foreach(test_name IN LISTS ZRPC_TESTS)
    add_executable(${test_name} ${test_name}.cpp)
    set_target_properties(${test_name} PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(${test_name} PRIVATE zrpc)
endforeach()

install(TARGETS ${ZRPC_TESTS}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINARY}
)
