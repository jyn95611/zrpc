cmake_minimum_required(VERSION 3.20)

project(zrpc)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR})

if(NOT DEFINED CMAKE_DEBUG_POSTFIX)
    set(CMAKE_DEBUG_POSTFIX _debug)
endif()

if(NOT DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(CMAKE_INSTALL_LIBRARY lib/debug)
    set(CMAKE_INSTALL_BINARY bin/debug)
elseif(${CMAKE_BUILD_TYPE} STREQUAL "Release")
    set(CMAKE_INSTALL_LIBRARY lib/release)
    set(CMAKE_INSTALL_BINARY bin/release)
endif()

set(ZRPC_SRC_DIR ${CMAKE_SOURCE_DIR}/src)

add_subdirectory(src/zrpc)
add_subdirectory(tests)
