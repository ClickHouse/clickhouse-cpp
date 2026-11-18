#include "tcp_server.h"

#include <clickhouse/base/socket.h>
#include <clickhouse/base/wire_format.h>
#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <future>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <thread>
#include <vector>

// for EAI_* error codes
#if defined(_win_)
#   include <ws2tcpip.h>
#else
#   include <netdb.h>
#   include <unistd.h>
#endif

using namespace clickhouse;

TEST(Socketcase, connecterror) {
    int port = 19978;
    NetworkAddress addr("localhost", std::to_string(port));
    LocalTcpServer server(port);
    server.start();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    try {
        Socket socket(addr);
    } catch (const std::system_error& e) {
        FAIL();
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    server.stop();
    try {
        Socket socket(addr);
        FAIL();
    } catch (const std::system_error& e) {
        ASSERT_NE(EINPROGRESS,e.code().value());
    }
}

TEST(Socketcase, timeoutrecv) {
    using Seconds = std::chrono::seconds;

    int port = 19979;
    NetworkAddress addr("localhost", std::to_string(port));
    LocalTcpServer server(port);
    server.start();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    try {
        Socket socket(addr, SocketTimeoutParams { Seconds(5), Seconds(5), Seconds(5) });

        std::unique_ptr<InputStream> ptr_input_stream = socket.makeInputStream();
        char buf[1024];
        ptr_input_stream->Read(buf, sizeof(buf));

    }
    catch (const std::system_error& e) {
#if defined(_unix_)
        auto expected = EAGAIN;
#else
        auto expected = WSAETIMEDOUT;
#endif
        ASSERT_EQ(expected, e.code().value());
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    server.stop();
}

TEST(Socketcase, ClientHandshakeTimesOutWithSilentServer) {
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;

    const int port = 19981;
    // The server accepts connections (through the listen backlog), but never sends data.
    LocalTcpServer server(port);
    server.start();

    const auto options = ClientOptions().SetHost("127.0.0.1").SetPort(port).SetSendRetries(1);
    const std::vector<ClientOptions> cases = {
        ClientOptions(options).SetConnectionHandshakeTimeout(500ms),
        // The handshake uses the shorter of the two timeouts.
        ClientOptions(options).SetConnectionHandshakeTimeout(60s).SetConnectionRecvTimeout(500ms),
    };

    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE("case " + std::to_string(i));
        const auto start = Clock::now();
        auto result = std::async(std::launch::async, [&options = cases[i]] { Client client(options); });
        const bool returned = result.wait_for(10s) == std::future_status::ready;
        if (!returned) {
            // Closing the listening socket resets the connection, so that the client returns.
            server.stop();
        }
        ASSERT_TRUE(returned) << "the client still waits for the Hello packet";
        EXPECT_LT(Clock::now() - start, 5s);
        EXPECT_THROW(result.get(), std::system_error);
    }
}

TEST(Socketcase, gaierror) {
    try {
        NetworkAddress addr("host.invalid", "80");  // never resolves
        FAIL();
    } catch (const std::system_error& e) {
        ASSERT_PRED1([](int error) { return error == EAI_NONAME || error == EAI_AGAIN || error == EAI_FAIL; }, e.code().value());
    }
}

TEST(Socketcase, connecttimeout) {
    using Clock = std::chrono::steady_clock;

    try {
        NetworkAddress("::1", "19980");
    } catch (const std::system_error& e) {
        GTEST_SKIP() << "missing IPv6 support";
    }

    NetworkAddress addr("100::1", "19980");  // "discard" IPv6 address

    const auto connect_start = Clock::now();
    try {
        Socket socket(addr, SocketTimeoutParams{std::chrono::milliseconds(100)});
        FAIL();
    } catch (const std::system_error& e) {
        const int error = e.code().value();
        if (error == ENETUNREACH || error == EHOSTUNREACH
#if defined(_win_)
            || error == WSAENETUNREACH
#endif
        ) {
            GTEST_SKIP() << "missing IPv6 support";
        }
#if defined(_win_)
        const auto expected = WSAETIMEDOUT;
#else
        const auto expected = ETIMEDOUT;
#endif
        EXPECT_EQ(expected, error);
        EXPECT_LT(Clock::now() - connect_start, std::chrono::seconds(5));
    }
}

// Test to verify that reading from empty socket doesn't hangs.
//TEST(Socketcase, ReadFromEmptySocket) {
//    const int port = 12345;
//    const NetworkAddress addr("127.0.0.1", std::to_string(port));

//    LocalTcpServer server(port);
//    server.start();

//    std::this_thread::sleep_for(std::chrono::seconds(1));

//    char buffer[1024];
//    Socket socket(addr);
//    socket.SetTcpNoDelay(true);
//    auto input = socket.makeInputStream();
//    input->Read(buffer, sizeof(buffer));
//}

namespace {

// RAII wrapper for the socket
class ScopedSocket {
public:
    explicit ScopedSocket(SOCKET socket) : handle(socket) {}

    ~ScopedSocket() {
        if (handle != static_cast<SOCKET>(-1)) {
#if defined(_win_)
            ::closesocket(handle);
#else
            ::close(handle);
#endif
        }
    }

    ScopedSocket(const ScopedSocket&) = delete;
    ScopedSocket& operator=(const ScopedSocket&) = delete;

    const SOCKET handle;
};

} // namespace

TEST(Socketcase, ReadEofThrowsProtocolError) {
    const ScopedSocket listener(::socket(AF_INET, SOCK_STREAM, 0));
    ASSERT_NE(static_cast<SOCKET>(-1), listener.handle);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    ASSERT_EQ(0, ::bind(listener.handle, reinterpret_cast<const sockaddr*>(&address), sizeof(address)));
    ASSERT_EQ(0, ::listen(listener.handle, 1));

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(0, ::getsockname(listener.handle, reinterpret_cast<sockaddr*>(&address), &address_size));

    const NetworkAddress client_address("127.0.0.1", std::to_string(ntohs(address.sin_port)));
    const auto timeout = std::chrono::seconds(5);
    Socket client(client_address, SocketTimeoutParams{timeout, timeout, timeout});

    // The listener's backlog lets connect complete before accept, without a thread.
    const ScopedSocket peer(::accept(listener.handle, nullptr, nullptr));
    ASSERT_NE(static_cast<SOCKET>(-1), peer.handle);

    const std::string payload = "hello";
    SocketOutput output(peer.handle);
    WireFormat::WriteBytes(output, payload.data(), payload.size());

    auto input = client.makeInputStream();
    char buf[16];
    ASSERT_TRUE(WireFormat::ReadBytes(*input, buf, payload.size()));
    ASSERT_EQ(payload, std::string(buf, payload.size()));

    // All data has been read; after FIN, the client's next nonempty recv returns 0 (EOF).
#if defined(_win_)
    ASSERT_EQ(0, ::shutdown(peer.handle, SD_SEND));
#else
    ASSERT_EQ(0, ::shutdown(peer.handle, SHUT_WR));
#endif

    // Seed an unrelated error; EOF must still be reported as ProtocolError.
#if defined(_win_)
    ::WSASetLastError(WSAECONNRESET);
#else
    errno = EIO;
#endif

    EXPECT_THROW(input->Read(buf, sizeof(buf)), ProtocolError);
}
