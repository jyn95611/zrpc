cmake_minimum_required(VERSION 3.20)

project(zrpc)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(ZrpcOptions)
include(ZrpcZmq)

add_subdirectory(src/zrpc)

if(ZRPC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
if(ZRPC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
