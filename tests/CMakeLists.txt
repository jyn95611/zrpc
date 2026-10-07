function(zrpc_add_test name)
    add_executable(${name} ${name}.cpp ${ARGN})
    set_target_properties(${name} PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_include_directories(${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/external
    )
    target_link_libraries(${name} PRIVATE zrpc zrpc_zmq)
    add_test(NAME ${name} COMMAND ${name})
endfunction()

# Hidden inside libzrpc. Tests that call them compile the .cpp in; the dylib does not export these symbols.
set(ZRPC_UTILS_SRC ${CMAKE_SOURCE_DIR}/src/zrpc/utils.cpp)
set(ZRPC_MESSAGE_SRC ${CMAKE_SOURCE_DIR}/src/zrpc/Message.cpp)

zrpc_add_test(test_utils ${ZRPC_UTILS_SRC})
zrpc_add_test(test_binser)
zrpc_add_test(test_service)
zrpc_add_test(test_rpc)
zrpc_add_test(test_pubsub)
zrpc_add_test(test_message ${ZRPC_MESSAGE_SRC} ${ZRPC_UTILS_SRC})
