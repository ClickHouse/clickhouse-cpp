// Live tests: they need a ClickHouse server. Set CLICKHOUSE_TEST_RUNNER_LIVE=1
// to run them. The server is CLICKHOUSE_HOST (default localhost), with the
// native port CLICKHOUSE_PORT_TCP (default 9000) and the HTTP port
// CLICKHOUSE_PORT_HTTP (default 8123).
//
// TestLiveRender compares the text that the shim writes from the values that
// clickhouse-cpp decoded with the server's own TabSeparated text.

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "shim.h"

using namespace clickhouse_test_runner;

namespace {

std::string Env(const char* name, const char* def) {
    const char* v = std::getenv(name);
    return v && *v ? v : def;
}

bool LiveEnabled() {
    return Env("CLICKHOUSE_TEST_RUNNER_LIVE", "") == "1";
}

#define REQUIRE_LIVE()                                                                                    \
    if (!LiveEnabled()) {                                                                                 \
        GTEST_SKIP() << "set CLICKHOUSE_TEST_RUNNER_LIVE=1 to run the tests against a ClickHouse server"; \
    }

/// The server's own TabSeparated text for a query: a minimal HTTP/1.0 POST to
/// the HTTP interface.
std::string ServerTSV(const std::string& query) {
    const std::string host = Env("CLICKHOUSE_HOST", "localhost");
    const std::string port = Env("CLICKHOUSE_PORT_HTTP", "8123");
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res     = nullptr;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0 || !res) {
        ADD_FAILURE() << "cannot resolve " << host;
        return {};
    }
    const int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0 || connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        freeaddrinfo(res);
        if (fd >= 0) {
            close(fd);
        }
        ADD_FAILURE() << "cannot connect to " << host << ":" << port;
        return {};
    }
    freeaddrinfo(res);
    const std::string request = "POST /?default_format=TabSeparated HTTP/1.0\r\nHost: " + host +
                                "\r\nContent-Length: " + std::to_string(query.size()) + "\r\n\r\n" + query;
    for (size_t sent = 0; sent < request.size();) {
        const ssize_t n = send(fd, request.data() + sent, request.size() - sent, 0);
        if (n <= 0) {
            close(fd);
            ADD_FAILURE() << "cannot send the HTTP request";
            return {};
        }
        sent += static_cast<size_t>(n);
    }
    std::string response;
    char buf[65536];
    for (ssize_t n; (n = recv(fd, buf, sizeof(buf), 0)) > 0;) {
        response.append(buf, static_cast<size_t>(n));
    }
    close(fd);
    const size_t body = response.find("\r\n\r\n");
    if (body == std::string::npos || response.compare(0, 12, "HTTP/1.0 200") != 0) {
        ADD_FAILURE() << "HTTP error for " << query << ": " << response;
        return {};
    }
    return response.substr(body + 4);
}

struct ShimResult {
    std::string out;
    std::string err;
    int code;
};

ShimResult RunShim(const std::string& query) {
    std::istringstream in;
    std::ostringstream out, err;
    const int code = RunMain(
        {"--host", Env("CLICKHOUSE_HOST", "localhost"), "--port", Env("CLICKHOUSE_PORT_TCP", "9000"), "--query", query}, in, out, err);
    return {out.str(), err.str(), code};
}

}  // namespace

TEST(Live, RenderMatchesServer) {
    REQUIRE_LIVE();
    const std::vector<std::string> exprs = {
        "toInt8(-1), toUInt8(255), toInt64(-9223372036854775808), toUInt64(18446744073709551615)",
        "toInt128(-170141183460469231731687303715884105728), toUInt128(340282366920938463463374607431768211455)",
        "arrayJoin([1e15, 1e21, 1.5e-7, 0.1 + 0.2, nan, inf, -inf, -0.])",
        "toFloat32(0.1), toFloat32(3.4028235e38), toFloat32(-1.5e-7)",
        "toDecimal32(1.50, 2), toDecimal64(-0.001, 5), toDecimal128(100, 3), toDecimal128('-12345678901234567890.123', 10)",
        "'a\\tb\\\\c\\nd''e\\0\\b\\f\\v\\x01', toFixedString('ab', 4), ''",
        "true, false, NULL, CAST(NULL AS Nullable(String))",
        "toDate('2020-01-02'), toDate32('1900-01-01'), toDate32('2299-12-31'), toDateTime('2020-01-01 10:00:00', 'UTC')",
        "toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo'), toDateTime64('2020-01-01 00:00:00.120', 3), "
        "toDateTime64('2020-01-01 00:00:00', 9, 'Asia/Tokyo')",
        "toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), toDateTime64('2299-12-31 23:59:59.999999', 6, 'UTC'), "
        "toDateTime64('1960-05-05 12:00:00.5', 1, 'Europe/Berlin')",
        "toDateTime(0, 'Fixed/UTC+05:30:00'), toDateTime(1700000000, 'Fixed/UTC-08:00:00'), "
        "toDateTime64(0, 3, 'Fixed/UTC+14:00:00'), [toDateTime(0, 'Fixed/UTC-01:15:00')]",
        "fromUnixTimestamp64Second(toInt64(1234567891011), 'UTC'), fromUnixTimestamp64Second(toInt64(1234567891011), 'Asia/Makassar'), "
        "fromUnixTimestamp64Milli(toInt64(9223372036854775807), 'UTC'), fromUnixTimestamp64Second(toInt64(-9223372036854775808), 'UTC')",
        "fromUnixTimestamp64Second(toInt64(-62167219201), 'UTC'), fromUnixTimestamp64Second(toInt64(-62135596800), 'UTC'), "
        "fromUnixTimestamp64Second(toInt64(253402300799), 'Asia/Tokyo'), fromUnixTimestamp64Second(toInt64(-62167219200), "
        "'America/New_York')",
        "fromDaysSinceYearZero32(2147483647), fromDaysSinceYearZero32(0), fromDaysSinceYearZero32(365), "
        "[fromDaysSinceYearZero32(2147483647)], [fromUnixTimestamp64Milli(toInt64(9223372036854775807), 'UTC')]",
        "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), toIPv6('::ffff:1.2.3.4'), toIPv6('::1.2.3.4'), "
        "toIPv6('2001:db8::1')",
        "[1, 2], ['a''b', NULL], [[1], []], [toDate('2020-01-01')], [toDateTime64('2020-01-01 00:00:00.5', 2)]",
        "(1, 'x', NULL), CAST((1, 'x') AS Tuple(a Int32, b String)), [(1, 'a')], ((1, 'a'), 2)",
        "CAST((1, NULL) AS Nullable(Tuple(Int32, Nullable(String)))), CAST(NULL AS Nullable(Tuple(Int32)))",
        "map('k', 1, 'a', 2), map(1, [2]), map('x', map('y', 1, 'z', 2)), map()",
        "CAST('a' AS Enum8('a' = 1, 'b' = 2)), [CAST('b' AS Enum16('a' = 1, 'b' = 2))], CAST('it''s' AS Enum8('it''s' = -1))",
        "toLowCardinality('x'), toLowCardinality(toNullable('y')), [toLowCardinality('z')], CAST(NULL AS LowCardinality(Nullable(String)))",
        "CAST(tuple(NULL, 'a') AS Tuple(x LowCardinality(Nullable(String)), y LowCardinality(String)))",
        "toLowCardinality(toUInt32(7)), toLowCardinality(toDate('2020-01-01')), toLowCardinality(toFixedString('ab', 3))",
        "toTime('12:34:56'), toTime('-1:02:03'), toTime64('12:00:00.5', 3), toTime64('-00:00:01.25', 2)",
        "(1.5, 2.5)::Point, [(1, 2), (3, 4)]::Ring, [[(1, 2), (3, 4)]]::Polygon, [[[(1, 2)]]]::MultiPolygon",
        "number, toString(number) FROM numbers(3)",
        "if(number % 5, NULL, toInt8OrZero(toString(number))) FROM numbers(6)",
        "toInt128(number) * -3, [toUInt128(number)] FROM numbers(3)",
        // A column or alias named `format` is not the FORMAT clause.
        "1 AS format ORDER BY format DESC",
        "1 AS format ORDER BY format ASC",
        "format FROM (SELECT 1 AS format)",
        "1 AS format FROM numbers(1)",
    };
    for (const auto& expr : exprs) {
        const std::string query = "SELECT " + expr;
        const auto want         = ServerTSV(query);
        const auto got          = RunShim(query);
        EXPECT_EQ(0, got.code) << query << "\n" << got.err;
        EXPECT_EQ(want, got.out) << query;
    }
}

TEST(Live, ValuesAndLineAsStringMatchServer) {
    REQUIRE_LIVE();
    // The server's own text of the same FORMAT clause.
    const std::vector<std::string> queries = {
        "SELECT number, toString(number), [number], NULL, 'a''b\\t' FROM numbers(3) FORMAT Values",
        "SELECT 1.5, -0., nan, inf, 1e21, toDecimal32(1.50, 2), true, toDate('2020-01-01'), toDateTime64(0, 3, 'UTC') FORMAT Values",
        "SELECT map(1, 2), (1, 'x'), toFixedString('ab', 3), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), "
        "CAST('a' AS Enum8('a' = 1)), toLowCardinality(toNullable('z')) FORMAT Values",
        "SELECT number FROM numbers(3) SETTINGS max_block_size = 1 FORMAT Values",
        "SELECT 1 WHERE 0 FORMAT Values",
        "SELECT 'a\\tb', NULL, [NULL], ['a\\tb'] FORMAT LineAsString",
        "SELECT toString(number) FROM numbers(3) FORMAT LineAsString",
    };
    for (const auto& query : queries) {
        const auto want = ServerTSV(query);
        const auto got  = RunShim(query);
        EXPECT_EQ(0, got.code) << query << "\n" << got.err;
        EXPECT_EQ(want, got.out) << query;
    }
}

TEST(Live, HeaderOfFormatMatchesServer) {
    REQUIRE_LIVE();
    // The header of a Raw format has no escapes, as its rows: the names, and
    // the types (the Enum type name has quotes and a backslash). Contrast:
    // TSVWithNames and TSVWithNamesAndTypes escape the header.
    const std::string names                = "SELECT 1 AS `a\\tb\\\\c'd`, 'x' AS `\xC3\xA9\\nf` FORMAT ";
    const std::string types                = "SELECT CAST('a' AS Enum8('a\\tb' = 1, 'a' = 2)) AS e FORMAT ";
    const std::vector<std::string> queries = {
        names + "TSVWithNames",         names + "TSVRawWithNames",         names + "TabSeparatedRawWithNamesAndTypes",
        types + "TSVWithNamesAndTypes", types + "TSVRawWithNamesAndTypes",
    };
    for (const auto& query : queries) {
        const auto want = ServerTSV(query);
        const auto got  = RunShim(query);
        EXPECT_EQ(0, got.code) << query << "\n" << got.err;
        EXPECT_EQ(want, got.out) << query;
    }
}

TEST(Live, SessionState) {
    REQUIRE_LIVE();
    // SET, temporary tables and inline INSERT data last for the whole
    // invocation, also after an error that a test hint expects.
    const std::string script =
        "SET max_threads = 3;\n"
        "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\n"
        "SELECT getSetting('max_threads');\n"
        "CREATE TEMPORARY TABLE t (x UInt8);\n"
        "INSERT INTO t VALUES (1), (2);\n"
        "INSERT INTO t FORMAT TSV\n"
        "3\n"
        "\n"
        "SELECT sum(x) FROM t;\n";
    const auto got = RunShim(script);
    EXPECT_EQ(0, got.code) << got.err;
    EXPECT_EQ("3\n6\n", got.out);
}

TEST(Live, Errors) {
    REQUIRE_LIVE();
    // An unexpected error stops the script, writes the error name to stderr,
    // and sets the exit code to the error code.
    auto got = RunShim("SELECT 1; SELECT * FROM no_such_table_xyz; SELECT 2;");
    EXPECT_EQ("1\n", got.out);
    EXPECT_EQ(60, got.code);
    EXPECT_NE(std::string::npos, got.err.find("UNKNOWN_TABLE")) << got.err;

    // A query that succeeds although the hint expects an error.
    got = RunShim("SELECT 1; -- { serverError UNKNOWN_TABLE }");
    EXPECT_NE(0, got.code);
    EXPECT_NE(std::string::npos, got.err.find("expected serverError UNKNOWN_TABLE")) << got.err;

    // A different error than the hint expects.
    got = RunShim("SELECT * FROM no_such_table_xyz; -- { serverError SYNTAX_ERROR }");
    EXPECT_EQ(60, got.code) << got.err;
}

TEST(Live, StatementsWithoutResult) {
    REQUIRE_LIVE();
    const auto got = RunShim(
        "CREATE TEMPORARY TABLE t (x UInt8); "
        "WITH 1 AS y INSERT INTO t SELECT y; "
        "SELECT * FROM t FORMAT TSVWithNamesAndTypes; "
        "SELECT 1 FORMAT Null; "
        "SELECT x FROM t WHERE 0 FORMAT TSVWithNames;");
    EXPECT_EQ(0, got.code) << got.err;
    EXPECT_EQ("x\nUInt8\n1\nx\n", got.out);
}

TEST(Live, InsertWithoutDataFailsAtOnce) {
    REQUIRE_LIVE();
    // The server waits for the INSERT data; Client::Execute never sends it.
    const auto got = RunShim("CREATE TEMPORARY TABLE t (x UInt8); INSERT INTO t VALUES");
    EXPECT_EQ(1, got.code);
    EXPECT_NE(std::string::npos, got.err.find("asks for the data of the INSERT")) << got.err;
}
