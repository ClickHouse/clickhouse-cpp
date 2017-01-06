#include "socket.h"

#include <stdexcept>
#include <system_error>
#include <memory.h>

#if !defined(_win_)
#   include <errno.h>
#   include <netdb.h>
#   include <unistd.h>
#endif

namespace clickhouse {
namespace net {

NetworkAddress::NetworkAddress(const std::string& host, const std::string& port)
    : info_(nullptr)
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    // TODO if not local addr hints.ai_flags |= AI_ADDRCONFIG;

    const int error = getaddrinfo(host.c_str(), port.c_str(), &hints, &info_);

    if (error) {
        throw std::system_error(errno, std::system_category());
    }
}

NetworkAddress::~NetworkAddress() {
    if (info_) {
        freeaddrinfo(info_);
    }
}

const struct addrinfo* NetworkAddress::Info() const {
    return info_;
}


SocketHolder::SocketHolder()
    : handle_(-1)
{
}

SocketHolder::SocketHolder(SOCKET s)
    : handle_(s)
{
}

SocketHolder::SocketHolder(SocketHolder&& other)
    : handle_(other.handle_)
{
    other.handle_ = -1;
}

SocketHolder::~SocketHolder() {
    Close();
}

void SocketHolder::Close() noexcept {
    if (handle_ != -1) {
#if defined(_win_)
        closesocket(handle_);
#else
        close(handle_);
#endif
        handle_ = -1;
    }
}

bool SocketHolder::Closed() const noexcept {
    return handle_ == -1;
}

SocketHolder& SocketHolder::operator = (SocketHolder&& other) noexcept {
    if (this != &other) {
        Close();

        handle_ = other.handle_;
        other.handle_ = -1;
    }

    return *this;
}

SocketHolder::operator SOCKET () const noexcept {
    return handle_;
}


SocketInput::SocketInput(SOCKET s)
    : s_(s)
{
}

SocketInput::~SocketInput() = default;

size_t SocketInput::DoRead(void* buf, size_t len) {
    const ssize_t ret = recv(s_, buf, (int)len, 0);

    if (ret >= 0) {
        return (size_t)ret;
    }

    throw std::runtime_error("can't receive string data");
}


SOCKET SocketConnect(const NetworkAddress& addr) {
    for (auto res = addr.Info(); res != nullptr; res = res->ai_next) {
        SOCKET s(socket(res->ai_family, res->ai_socktype, res->ai_protocol));

        if (s == -1) {
            continue;
        }

        if (connect(s, res->ai_addr, (int)res->ai_addrlen)) {
            if (errno == EINPROGRESS ||
                errno == EAGAIN ||
                errno == EWOULDBLOCK)
            {
                pollfd fd;
                fd.fd = s;
                fd.events = POLLOUT;
                int rval = Poll(&fd, 1, 1000);

                if (rval > 0) {
                    int opt;
                    socklen_t len = sizeof(opt);
                    getsockopt(s, SOL_SOCKET, SO_ERROR, (char*)&opt, &len);

                    return opt;
                } else {
                    continue;
                }
            }
        }

        return s;
    }

    return -1;
}


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
