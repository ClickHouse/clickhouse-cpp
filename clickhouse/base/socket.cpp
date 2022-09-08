#include "socket.h"
#include "singleton.h"
#include "../client.h"

#include <assert.h>
#include <stdexcept>
#include <system_error>
#include <unordered_set>
#include <memory.h>
#include <thread>

#if !defined(_win_)
#   include <errno.h>
#   include <fcntl.h>
#   include <netdb.h>
#   include <netinet/tcp.h>
#   include <signal.h>
#   include <unistd.h>
#endif

namespace clickhouse {

#if defined(_win_)
char const* windowsErrorCategory::name() const noexcept {
    return "WindowsSocketError";
}

std::string windowsErrorCategory::message(int c) const {
    char error[UINT8_MAX];
    auto len = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM, nullptr, static_cast<DWORD>(c), 0, error, sizeof(error), nullptr);
    if (len == 0) {
        return "unknown";
    }
    while (len && (error[len - 1] == '\r' || error[len - 1] == '\n')) {
        --len;
    }
    return std::string(error, len);
}

windowsErrorCategory const& windowsErrorCategory::category() {
    static windowsErrorCategory c;
    return c;
}
#endif

namespace {

class LocalNames : public std::unordered_set<std::string> {
public:
    LocalNames() {
        emplace("localhost");
        emplace("localhost.localdomain");
        emplace("localhost6");
        emplace("localhost6.localdomain6");
        emplace("::1");
        emplace("127.0.0.1");
    }

    inline bool IsLocalName(const std::string& name) const noexcept {
        return find(name) != end();
    }
};

inline int getSocketErrorCode() {
#if defined(_win_)
    return WSAGetLastError();
#else
    return errno;
#endif
}

const std::error_category& getErrorCategory() noexcept {
#if defined(_win_)
    return windowsErrorCategory::category();
#else
    return std::system_category();
#endif
}

void SetNonBlock(SOCKET fd, bool value) {
#if defined(_unix_) || defined(__CYGWIN__)
    int flags;
    int ret;
    #if defined(O_NONBLOCK)
        if ((flags = fcntl(fd, F_GETFL, 0)) == -1)
            flags = 0;
        if (value) {
            flags |= O_NONBLOCK;
        } else {
            flags &= ~O_NONBLOCK;
        }
        ret = fcntl(fd, F_SETFL, flags);
    #else
        flags = value;
        return ioctl(fd, FIOBIO, &flags);
    #endif
    if (ret == -1) {
        throw std::system_error(getSocketErrorCode(), getErrorCategory(), "fail to set nonblocking mode");
    }
#elif defined(_win_)
    unsigned long inbuf = value;
    unsigned long outbuf = 0;
    DWORD written = 0;

    if (!inbuf) {
        WSAEventSelect(fd, nullptr, 0);
    }

    if (WSAIoctl(fd, FIONBIO, &inbuf, sizeof(inbuf), &outbuf, sizeof(outbuf), &written, 0, 0) == SOCKET_ERROR) {
        throw std::system_error(getSocketErrorCode(), getErrorCategory(), "fail to set nonblocking mode");
    }
#endif
}

ssize_t Poll(struct pollfd* fds, int nfds, int timeout) noexcept {
#if defined(_win_)
    return WSAPoll(fds, nfds, timeout);
#else
    return poll(fds, nfds, timeout);
#endif
}

SOCKET SocketConnect(const NetworkAddress& addr) {
    int last_err = 0;
    for (auto res = addr.Info(); res != nullptr; res = res->ai_next) {
        SOCKET s(socket(res->ai_family, res->ai_socktype, res->ai_protocol));

        if (s == -1) {
            continue;
        }

        SetNonBlock(s, true);

        if (connect(s, res->ai_addr, (int)res->ai_addrlen) != 0) {
            int err = getSocketErrorCode();
            if (
                err == EINPROGRESS || err == EAGAIN || err == EWOULDBLOCK
#if defined(_win_)
                || err == WSAEWOULDBLOCK || err == WSAEINPROGRESS
#endif
            ) {
                pollfd fd;
                fd.fd = s;
                fd.events = POLLOUT;
                fd.revents = 0;
                ssize_t rval = Poll(&fd, 1, 5000);

                if (rval == -1) {
                    throw std::system_error(getSocketErrorCode(), getErrorCategory(), "fail to connect");
                }
                if (rval > 0) {
                    socklen_t len = sizeof(err);
                    getsockopt(s, SOL_SOCKET, SO_ERROR, (char*)&err, &len);

                    if (!err) {
                        SetNonBlock(s, false);
                        return s;
                    }
                   last_err = err;
                }
            }
        } else {
            SetNonBlock(s, false);
            return s;
        }
    }
    if (last_err > 0) {
        throw std::system_error(last_err, getErrorCategory(), "fail to connect");
    }
    throw std::system_error(getSocketErrorCode(), getErrorCategory(), "fail to connect");
}

} // namespace

NetworkAddress::NetworkAddress(const std::string& host, const std::string& port)
    : host_(host)
    , info_(nullptr)
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    // using AI_ADDRCONFIG on windows will cause getaddrinfo to return WSAHOST_NOT_FOUND
    // for more information, see https://github.com/ClickHouse/clickhouse-cpp/issues/195
#if defined(_unix_) 
    if (!Singleton<LocalNames>()->IsLocalName(host)) {
        // https://linux.die.net/man/3/getaddrinfo
        // If hints.ai_flags includes the AI_ADDRCONFIG flag,
        // then IPv4 addresses are returned in the list pointed to by res only
        // if the local system has at least one IPv4 address configured,
        // and IPv6 addresses are only returned if the local system
        // has at least one IPv6 address configured.
        // The loopback address is not considered for this case
        // as valid as a configured address.
        hints.ai_flags |= AI_ADDRCONFIG;
    }
#endif

    const int error = getaddrinfo(host.c_str(), port.c_str(), &hints, &info_);

    if (error) {
        throw std::system_error(getSocketErrorCode(), getErrorCategory());
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
const std::string & NetworkAddress::Host() const {
    return host_;
}


SocketBase::~SocketBase() = default;

SocketFactory::~SocketFactory() = default;

void SocketFactory::sleepFor(const std::chrono::milliseconds& duration) {
    std::this_thread::sleep_for(duration);
}


Socket::Socket(const NetworkAddress& addr)
    : handle_(SocketConnect(addr))
{}

Socket::Socket(Socket&& other) noexcept
    : handle_(other.handle_)
{
    other.handle_ = -1;
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        Close();

        handle_ = other.handle_;
        other.handle_ = -1;
    }

    return *this;
}

Socket::~Socket() {
    Close();
}

void Socket::Close() {
    if (handle_ != -1) {
#if defined(_win_)
        closesocket(handle_);
#else
        close(handle_);
#endif
        handle_ = -1;
    }
}

void Socket::SetTcpKeepAlive(int idle, int intvl, int cnt) noexcept {
    int val = 1;

#if defined(_unix_)
    setsockopt(handle_, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val));
#   if defined(_linux_)
        setsockopt(handle_, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
#   elif defined(_darwin_)
        setsockopt(handle_, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle));
#   else
#       error "platform is not supported"
#   endif
    setsockopt(handle_, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    setsockopt(handle_, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
#else
    setsockopt(handle_, SOL_SOCKET, SO_KEEPALIVE, (const char*)&val, sizeof(val));
    std::ignore = idle = intvl = cnt;
#endif
}

void Socket::SetTcpNoDelay(bool nodelay) noexcept {
    int val = nodelay;
#if defined(_unix_)
    setsockopt(handle_, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
#else
    setsockopt(handle_, IPPROTO_TCP, TCP_NODELAY, (const char*)&val, sizeof(val));
#endif
}

std::unique_ptr<InputStream> Socket::makeInputStream() const {
    return std::make_unique<SocketInput>(handle_);
}

std::unique_ptr<OutputStream> Socket::makeOutputStream() const {
    return std::make_unique<SocketOutput>(handle_);
}

NonSecureSocketFactory::~NonSecureSocketFactory()  {}

std::unique_ptr<SocketBase> NonSecureSocketFactory::connect(const ClientOptions &opts) {
    const auto address = NetworkAddress(opts.host, std::to_string(opts.port));

    auto socket = doConnect(address);
    setSocketOptions(*socket, opts);

    return socket;
}

std::unique_ptr<Socket> NonSecureSocketFactory::doConnect(const NetworkAddress& address) {
    return std::make_unique<Socket>(address);
}

void NonSecureSocketFactory::setSocketOptions(Socket &socket, const ClientOptions &opts) {
    if (opts.tcp_keepalive) {
        socket.SetTcpKeepAlive(
                static_cast<int>(opts.tcp_keepalive_idle.count()),
                static_cast<int>(opts.tcp_keepalive_intvl.count()),
                static_cast<int>(opts.tcp_keepalive_cnt));
    }
    if (opts.tcp_nodelay) {
        socket.SetTcpNoDelay(opts.tcp_nodelay);
    }
}

SocketInput::SocketInput(SOCKET s)
    : s_(s)
{
}

SocketInput::~SocketInput() = default;

size_t SocketInput::DoRead(void* buf, size_t len) {
    const ssize_t ret = ::recv(s_, (char*)buf, (int)len, 0);

    if (ret > 0) {
        return (size_t)ret;
    }

    if (ret == 0) {
        throw std::system_error(getSocketErrorCode(), getErrorCategory(), "closed");
    }

    throw std::system_error(getSocketErrorCode(), getErrorCategory(), "can't receive string data");
}

bool SocketInput::Skip(size_t /*bytes*/) {
    return false;
}


SocketOutput::SocketOutput(SOCKET s)
    : s_(s)
{
}

SocketOutput::~SocketOutput() = default;

size_t SocketOutput::DoWrite(const void* data, size_t len) {
#if defined (_linux_)
    static const int flags = MSG_NOSIGNAL;
#else
    static const int flags = 0;
#endif

    if (::send(s_, (const char*)data, (int)len, flags) != (int)len) {
        throw std::system_error(getSocketErrorCode(), getErrorCategory(), "fail to send " + std::to_string(len) + " bytes of data");
    }

    return len;
}


NetrworkInitializer::NetrworkInitializer() {
    struct NetrworkInitializerImpl {
        NetrworkInitializerImpl() {
#if defined (_win_)
            WSADATA data;
            const int result = WSAStartup(MAKEWORD(2, 2), &data);
            if (result) {
                assert(false);
                exit(-1);
            }
#elif defined(_unix_)
            signal(SIGPIPE, SIG_IGN);
#endif
        }
    };


    (void)Singleton<NetrworkInitializerImpl>();
}

}
