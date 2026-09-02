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

#if !defined(_win_)
#   include <sys/socket.h>
#   include <unistd.h>

// Regression test for issue #487.
//
// On a clean peer close, `recv()` returns 0, which is EOF, not an error.
// POSIX does NOT require `errno` to be set when `recv()` returns 0, so reading
// `errno` at that point yields a stale value from a previous syscall. Prior
// to the fix, `SocketInput::DoRead` surfaced that stale `errno` to the caller
// (e.g. "Operation now in progress" if the last failing call was the
// non-blocking `connect()`), making the exception message non-deterministic
// and misleading.
//
// The fix reports `ECONNRESET` with a fixed message instead. This test
// drives a clean close via `socketpair(2)`, seeds `errno` to a known value
// that is NOT `ECONNRESET`, and asserts the resulting exception's
// `error_code` is exactly `ECONNRESET`.
TEST(Socketcase, recvReturnsZeroReportsConnResetNotStaleErrno) {
    int sv[2];
    ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM, 0, sv));

    // Seed `errno` to a known stale value that must NOT leak into the
    // exception. On Linux, closing an invalid fd sets `errno = EBADF` (9),
    // which is clearly distinct from `ECONNRESET` (104).
    if (::close(-1) != -1) {
        // Sanity guard: `close(-1)` must fail; if it doesn't, the test
        // cannot guarantee `errno` is set as expected.
        ::close(sv[0]);
        ::close(sv[1]);
        FAIL() << "close(-1) unexpectedly succeeded; cannot seed errno";
    }
    ASSERT_EQ(EBADF, errno);

    SocketInput input(sv[0]);
    // Close the peer side: the next `recv()` on sv[0] returns 0 (EOF).
    ::close(sv[1]);

    char buf[16];
    try {
        input.Read(buf, sizeof(buf));
        ::close(sv[0]);
        FAIL() << "expected std::system_error on clean peer close";
    } catch (const std::system_error& e) {
        ::close(sv[0]);
        EXPECT_EQ(ECONNRESET, e.code().value())
            << "stale errno leaked into the exception: " << e.code().value();
        EXPECT_NE(EBADF, e.code().value())
            << "regression: stale errno was surfaced instead of ECONNRESET";
    }
}
#endif  // !defined(_win_)
