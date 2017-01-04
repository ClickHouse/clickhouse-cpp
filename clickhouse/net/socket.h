#pragma once

#include "base/platform.h"

#include <cstddef>

#if defined(_win_)
#   pragma comment(lib, "Ws2_32.lib")
#   include <Winsock2.h>
#else
#   include <sys/types.h>
#   include <sys/socket.h>

#   if !defined(SOCKET)
#       define SOCKET int
#   endif
#endif

namespace clickhouse {
namespace net {

ssize_t Poll(struct pollfd fds[], int nfds, int timeout) noexcept;

}
}
