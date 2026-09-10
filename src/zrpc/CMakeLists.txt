file(GLOB_RECURSE ZRPC_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.h ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(zrpc SHARED ${ZRPC_SRCS})
set_target_properties(zrpc PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

target_compile_definitions(zrpc PRIVATE ZRPC_LIBRARY)

if(NOT MSVC)
    target_compile_options(zrpc PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
endif()

if(WIN32)
    target_compile_definitions(zrpc PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()
if(MSVC)
    target_compile_options(zrpc PRIVATE /utf-8)
    target_compile_definitions(zrpc PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

if(WIN32)
    set(_ZMQ_DIR_DEFAULT "${CMAKE_SOURCE_DIR}/../libzmq")
    set(_CPPZMQ_DIR_DEFAULT "${CMAKE_SOURCE_DIR}/../cppzmq")
else()
    set(_ZMQ_DIR_DEFAULT "/Users/jiaoyining/workspace/projects/cpp_pro/zmq/libzmq")
    set(_CPPZMQ_DIR_DEFAULT "/opt/homebrew/Cellar/cppzmq/4.10.0")
endif()
set(ZMQ_DIR "${_ZMQ_DIR_DEFAULT}" CACHE PATH "libzmq root directory")
set(CPPZMQ_DIR "${_CPPZMQ_DIR_DEFAULT}" CACHE PATH "cppzmq root directory")

target_include_directories(zrpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)
target_include_directories(zrpc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(zrpc PRIVATE ${ZMQ_DIR}/include)
target_include_directories(zrpc PRIVATE ${CPPZMQ_DIR}/include)
target_link_directories(zrpc PUBLIC ${ZMQ_DIR}/build/lib/release)

if(WIN32)
    target_link_libraries(zrpc PUBLIC libzmq)
else()
    target_link_libraries(zrpc PUBLIC zmq pthread)
endif()

set(ZRPC_HEADERS
    zrpc_global.h
    Context.h
    Server.h
    Channel.h
    Call.h
    PubSub.h)
foreach(header IN LISTS ZRPC_HEADERS)
    list(APPEND ZRPC_HEADER_FILES ${CMAKE_CURRENT_SOURCE_DIR}/${header})
endforeach()
install(FILES ${ZRPC_HEADER_FILES} DESTINATION include/zrpc)
install(TARGETS zrpc
    RUNTIME DESTINATION ${CMAKE_INSTALL_LIBRARY}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBRARY}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBRARY}
)
