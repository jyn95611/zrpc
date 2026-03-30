add_executable(test_zrpc test_zrpc.cpp)
add_executable(test_zrpc_client test_zrpc_client.cpp)
add_executable(test_zrpc_server test_zrpc_server.cpp)
add_executable(test_zrpc_pub test_zrpc_pub.cpp)
add_executable(test_zrpc_sub test_zrpc_sub.cpp)
add_executable(test_zrpc_pub_sub test_zrpc_pub_sub.cpp)

set(CPP_DIR /Users/jiaoyining/workspace/project/cpp_pro)
set(ZMQ_DIR ${CPP_DIR}/zmq/libzmq)
set(GTEST_DIR ${CPP_DIR}/gtest/googletest)
function(setTestTargetpProperties target)
    set_target_properties(${target} PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    
    target_include_directories(
        ${target} PRIVATE 
        ${CMAKE_SOURCE_DIR}/include
        ${GTEST_DIR}/include
    )

    target_link_directories(
        ${target} PRIVATE 
        ${GTEST_DIR}/lib
    )
    target_link_libraries(${target} zrpc gtest gtest_main)
endfunction()

setTestTargetpProperties(test_zrpc)
setTestTargetpProperties(test_zrpc_client)
setTestTargetpProperties(test_zrpc_server)
setTestTargetpProperties(test_zrpc_pub)
setTestTargetpProperties(test_zrpc_sub)
setTestTargetpProperties(test_zrpc_pub_sub)

install(TARGETS
    test_zrpc
    test_zrpc_client test_zrpc_server
    test_zrpc_pub test_zrpc_sub test_zrpc_pub_sub
    DESTINATION ${CMAKE_INSTALL_BINARY})
