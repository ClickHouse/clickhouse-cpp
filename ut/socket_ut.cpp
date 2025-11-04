#include "tcp_server.h"

#include <clickhouse/base/socket.h>
#include <gtest/gtest.h>

#include <iostream>
#include <stdio.h>
#include <string.h>
#include <thread>

// for EAI_* error codes
#if defined(_win_)
#   include <ws2tcpip.h>
#else
#   include <netdb.h>
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

#if defined(_unix_)

#include "unix_socket_server.h"
#include <clickhouse/client.h>

TEST(Socketcase, UnixSocketConnect) {
    const std::string socket_path = "/tmp/test_clickhouse_cpp_unix_socket.sock";
    LocalUnixSocketServer server(socket_path);
    server.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    try {
        // Test connection via NonSecureSocketFactory
        ClientOptions opts;
        opts.SetSocketPath(socket_path);
        Endpoint endpoint;
        endpoint.socket_path = socket_path;
        
        NonSecureSocketFactory factory;
        auto socket_base = factory.connect(opts, endpoint);
        EXPECT_NE(nullptr, socket_base);
        SUCCEED();
    } catch (const std::system_error& e) {
        FAIL() << "Failed to connect to UNIX socket: " << e.what();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    server.stop();
}

TEST(Socketcase, UnixSocketConnectError) {
    const std::string socket_path = "/tmp/test_clickhouse_cpp_unix_socket_nonexistent.sock";
    
    try {
        ClientOptions opts;
        opts.SetSocketPath(socket_path);
        opts.SetConnectionConnectTimeout(std::chrono::milliseconds(100));
        Endpoint endpoint;
        endpoint.socket_path = socket_path;
        
        NonSecureSocketFactory factory;
        auto socket_base = factory.connect(opts, endpoint);
        FAIL() << "Should have failed to connect to non-existent UNIX socket";
    } catch (const std::system_error& e) {
        // Expected to fail
        EXPECT_TRUE(e.code().value() == ECONNREFUSED || e.code().value() == ENOENT);
    }
}

TEST(Socketcase, UnixSocketPathTooLong) {
    const std::string long_path(200, 'a'); // Longer than UNIX_PATH_MAX (typically 108)
    
    try {
        ClientOptions opts;
        opts.SetSocketPath(long_path);
        Endpoint endpoint;
        endpoint.socket_path = long_path;
        
        NonSecureSocketFactory factory;
        auto socket_base = factory.connect(opts, endpoint);
        FAIL() << "Should have failed with path too long error";
    } catch (const std::system_error& e) {
        EXPECT_EQ(EINVAL, e.code().value());
    }
}

TEST(ClientUnixSocket, UnixSocketEndpoint) {
    // This test requires a real ClickHouse server listening on a UNIX socket
    // For now, we'll just test that the endpoint structure works correctly
    Endpoint endpoint;
    endpoint.host = "localhost";
    endpoint.port = 9000;
    endpoint.socket_path = "/tmp/test.sock";
    
    Endpoint endpoint2;
    endpoint2.host = "localhost";
    endpoint2.port = 9000;
    endpoint2.socket_path = "/tmp/test.sock";
    
    EXPECT_EQ(endpoint, endpoint2);
    
    Endpoint endpoint3;
    endpoint3.host = "localhost";
    endpoint3.port = 9000;
    endpoint3.socket_path = "/tmp/different.sock";
    
    EXPECT_FALSE(endpoint == endpoint3);
}

TEST(ClientUnixSocket, UnixSocketClientOptions) {
    ClientOptions opts;
    opts.SetSocketPath("/tmp/test.sock");
    EXPECT_EQ("/tmp/test.sock", opts.socket_path);
    
    opts.SetHost("localhost");
    opts.SetPort(9000);
    opts.SetSocketPath("/tmp/test.sock");
    
    // socket_path should take precedence
    EXPECT_EQ("/tmp/test.sock", opts.socket_path);
    EXPECT_EQ("localhost", opts.host);
    EXPECT_EQ(9000u, opts.port);
}

#endif // defined(_unix_)
