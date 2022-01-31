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
#   include <arpa/inet.h>
#   include <sys/types.h>
#   include <sys/socket.h>
#   include <poll.h>

#   if !defined(SOCKET)
#       define SOCKET int
#   endif
#endif

#include <memory>


struct addrinfo;

namespace clickhouse {

/** Address of a host to establish connection to.
 *
 */
class NetworkAddress {
public:
    explicit NetworkAddress(const std::string& host,
                            const std::string& port = "0");
    ~NetworkAddress();

    const struct addrinfo* Info() const;
    const std::string & Host() const;

private:
    const std::string host_;
    struct addrinfo* info_;
};

class SocketBase {
public:
    virtual ~SocketBase();

    virtual std::unique_ptr<InputStream> makeInputStream() const = 0;
    virtual std::unique_ptr<OutputStream> makeOutputStream() const = 0;
};


class Socket : public SocketBase {
public:
    Socket(const NetworkAddress& addr);
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    ~Socket() override;

    /// @params idle the time (in seconds) the connection needs to remain
    ///         idle before TCP starts sending keepalive probes.
    /// @params intvl the time (in seconds) between individual keepalive probes.
    /// @params cnt the maximum number of keepalive probes TCP should send
    ///         before dropping the connection.
    void SetTcpKeepAlive(int idle, int intvl, int cnt) noexcept;

    /// @params nodelay whether to enable TCP_NODELAY
    void SetTcpNoDelay(bool nodelay) noexcept;

    std::unique_ptr<InputStream> makeInputStream() const override;
    std::unique_ptr<OutputStream> makeOutputStream() const override;

protected:
    Socket(const Socket&) = delete;
    Socket& operator = (const Socket&) = delete;
    void Close();

    SOCKET handle_;
};


class SocketInput : public InputStream {
public:
    explicit SocketInput(SOCKET s);
    ~SocketInput();

protected:
    bool Skip(size_t bytes) override;
    size_t DoRead(void* buf, size_t len) override;

private:
    SOCKET s_;
};

class SocketOutput : public OutputStream {
public:
    explicit SocketOutput(SOCKET s);
    ~SocketOutput();

protected:
    size_t DoWrite(const void* data, size_t len) override;

private:
    SOCKET s_;
};

static struct NetrworkInitializer {
    NetrworkInitializer();
} gNetrworkInitializer;

}
