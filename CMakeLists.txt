cmake_minimum_required(VERSION 3.20)

project(zrpc)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR})

if(NOT DEFINED CMAKE_DEBUG_POSTFIX)
    set(CMAKE_DEBUG_POSTFIX _debug)
endif()

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(ZRPC_CONFIG_DIR $<IF:$<CONFIG:Debug>,debug,release>)
set(CMAKE_INSTALL_LIBRARY lib/${ZRPC_CONFIG_DIR})
set(CMAKE_INSTALL_BINARY bin/${ZRPC_CONFIG_DIR})

# Binaries install to bin/{debug,release}; libraries to lib/{debug,release}.
# Shared libs use @rpath/$ORIGIN, so installed binaries must record that path.
if(APPLE)
    set(CMAKE_INSTALL_RPATH "@loader_path;@loader_path/../../lib/${ZRPC_CONFIG_DIR}")
elseif(UNIX)
    set(CMAKE_INSTALL_RPATH "\$ORIGIN;\$ORIGIN/../../lib/${ZRPC_CONFIG_DIR}")
endif()
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(ZRPC_SRC_DIR ${CMAKE_SOURCE_DIR}/src)

add_subdirectory(src/zrpc)
add_subdirectory(tests)
