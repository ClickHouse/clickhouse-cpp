#pragma once

#include "platform.h"
#include "input.h"
#include "output.h"

#include <cstddef>
#include <string>

#if defined(_win_)
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
#include <system_error>

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

#if defined(_win_)

class windowsErrorCategory : public std::error_category {
public:
    char const* name() const noexcept override final;
    std::string message(int c) const override final;

    static windowsErrorCategory const& category();
};

#endif

class Socket {
public:
    Socket(const NetworkAddress& addr);
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    virtual ~Socket();

    /// @params idle the time (in seconds) the connection needs to remain
    ///         idle before TCP starts sending keepalive probes.
    /// @params intvl the time (in seconds) between individual keepalive probes.
    /// @params cnt the maximum number of keepalive probes TCP should send
    ///         before dropping the connection.
    void SetTcpKeepAlive(int idle, int intvl, int cnt) noexcept;

    /// @params nodelay whether to enable TCP_NODELAY
    void SetTcpNoDelay(bool nodelay) noexcept;

    virtual std::unique_ptr<InputStream> makeInputStream() const;
    virtual std::unique_ptr<OutputStream> makeOutputStream() const;

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
