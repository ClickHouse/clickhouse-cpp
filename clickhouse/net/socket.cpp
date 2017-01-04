#include "socket.h"

namespace clickhouse {
namespace net {

ssize_t Poll(struct pollfd* fds, int nfds, int timeout) noexcept {
#if defined(_win_)
    int rval = WSAPoll(fds, nfds, timeout);
#else
    return poll(fds, nfds, timeout);
#endif
    return -1;
}

}
}
