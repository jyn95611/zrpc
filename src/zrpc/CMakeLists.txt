file(GLOB ZRPC_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(zrpc SHARED ${ZRPC_SRCS})
set_target_properties(zrpc PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

target_compile_definitions(zrpc PRIVATE ZRPC_LIBRARY)
if(WIN32)
    target_compile_definitions(zrpc PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()
if(MSVC)
    target_compile_options(zrpc PRIVATE /utf-8)
    target_compile_definitions(zrpc PRIVATE _CRT_SECURE_NO_WARNINGS)
else()
    target_compile_options(zrpc PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
endif()

target_include_directories(zrpc
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/external
)
target_link_libraries(zrpc PRIVATE zrpc_zmq)

install(FILES
    zrpc_global.h
    Context.h
    Server.h
    Client.h
    Call.h
    PubSub.h
    DESTINATION include/zrpc
)
install(TARGETS zrpc
    RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
