file(GLOB_RECURSE ZRPC_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.h ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(zrpc SHARED ${ZRPC_SRCS})
set_target_properties(zrpc PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

set(ZMQ_DIR /Users/jiaoyining/workspace/project/cpp_pro/zmq/libzmq)
set(CPPZMQ_DIR /opt/homebrew/Cellar/cppzmq/4.10.0)

target_include_directories(zrpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/zrpc>
    $<INSTALL_INTERFACE:include>
)
#target_include_directories(zrpc PRIVATE ${ZRPC_SRC_DIR})
target_include_directories(zrpc PRIVATE ${ZMQ_DIR}/include)
target_include_directories(zrpc PRIVATE ${CPPZMQ_DIR}/include)
target_link_directories(zrpc PUBLIC ${ZMQ_DIR}/build/lib/release)
target_link_libraries(zrpc zmq pthread)

set(ZRPC_HEADERS
    Context.h
    Server.h
    Channel.h
    Rpc.h
    PubSub.h)
install(FILES ${ZRPC_HEADERS} DESTINATION include/zrpc)
install(TARGETS zrpc DESTINATION ${CMAKE_INSTALL_LIBRARY})


