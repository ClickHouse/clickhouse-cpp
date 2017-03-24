#pragma once

#include "input.h"
#include "output.h"
#include "platform.h"

#include <cstddef>
#include <string>

#if defined(_win_)
#   pragma comment(lib, "Ws2_32.lib")

#   include <winsock2.h>
#   include <ws2tcpip.h>
#else
#   include <sys/types.h>
#   include <sys/socket.h>
#   include <poll.h>

#   if !defined(SOCKET)
#       define SOCKET int
#   endif
#endif

struct addrinfo;

namespace clickhouse {

/**
 *
 */
class NetworkAddress {
public:
    explicit NetworkAddress(const std::string& host,
                            const std::string& port = "0");
    ~NetworkAddress();

    const struct addrinfo* Info() const;

private:
    struct addrinfo* info_;
};


class SocketHolder {
public:
    SocketHolder();
    SocketHolder(SOCKET s);
    SocketHolder(SocketHolder&& other);

    ~SocketHolder();

    void Close() noexcept;

    bool Closed() const noexcept;

    SocketHolder& operator = (SocketHolder&& other) noexcept;

    operator SOCKET () const noexcept;

private:
    SocketHolder(const SocketHolder&) = delete;
    SocketHolder& operator = (const SocketHolder&) = delete;

    SOCKET handle_;
};


/**
 *
 */
class SocketInput : public InputStream {
public:
    explicit SocketInput(SOCKET s);
    ~SocketInput();

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    SOCKET s_;
};

class SocketOutput : public OutputStream {
public:
    explicit SocketOutput(SOCKET s);
    ~SocketOutput();

protected:
    void DoWrite(const void* data, size_t len) override;

private:
    SOCKET s_;
};

static struct NetrworkInitializer {
    NetrworkInitializer();
} gNetrworkInitializer;

///
SOCKET SocketConnect(const NetworkAddress& addr);

ssize_t Poll(struct pollfd* fds, int nfds, int timeout) noexcept;

}
