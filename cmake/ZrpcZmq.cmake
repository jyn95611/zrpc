set(ZMQ_DIR "${CMAKE_SOURCE_DIR}/../libzmq" CACHE PATH "libzmq source/build tree")
set(ZMQ_LIBDIR ${ZMQ_DIR}/lib/${ZRPC_CONFIG_DIR})

add_library(zrpc_zmq INTERFACE)
target_include_directories(zrpc_zmq INTERFACE ${ZMQ_DIR}/include)
target_link_directories(zrpc_zmq INTERFACE ${ZMQ_LIBDIR})
if(WIN32)
    target_link_libraries(zrpc_zmq INTERFACE libzmq)
else()
    target_link_libraries(zrpc_zmq INTERFACE zmq pthread)
endif()

list(APPEND CMAKE_INSTALL_RPATH ${ZMQ_LIBDIR})
