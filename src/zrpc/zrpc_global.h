#pragma once

#if defined(_WIN32) || defined(_WIN64)
#  if defined(ZRPC_LIBRARY)
#    define ZRPC_EXPORT __declspec(dllexport)
#  else
#    define ZRPC_EXPORT __declspec(dllimport)
#  endif
#else
#  if defined(ZRPC_LIBRARY)
#    define ZRPC_EXPORT __attribute__((visibility("default")))
#  else
#    define ZRPC_EXPORT
#  endif
#endif
