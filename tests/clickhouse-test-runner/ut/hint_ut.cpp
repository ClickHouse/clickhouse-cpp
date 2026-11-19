#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "error_codes.h"
#include "hint.h"
#include "runner.h"

using namespace clickhouse_test_runner;

TEST(ParseTestHint, Cases) {
    struct Case {
        const char* name;
        const char* in;
        std::vector<int> server;
        std::vector<int> client;
    };
    const std::vector<Case> cases = {
        {"server error by name", "SELECT 1; -- { serverError UNKNOWN_TABLE }", {60}, {}},
        {"server error by code", "SELECT 1; -- { serverError 60 }", {60}, {}},
        {"several codes", "SELECT 1; -- { serverError 153, ILLEGAL_DIVISION }", {153, 153}, {}},
        {"client error", "SELECT; -- { clientError SYNTAX_ERROR }", {}, {62}},
        {"error sets both", "SELECT 1; -- { error 62 }", {62}, {62}},
        {"block comment", "SELECT 1 /* { serverError 60 } */;", {60}, {}},
        {"text after the hint", "SELECT 1; -- { serverError 60 } because there is no table", {60}, {}},
        {"leading hint is ignored", "-- { serverError 60 }\nSELECT 1;", {}, {}},
        {"braces in a string are not a hint", "SELECT '-- { serverError 60 }';", {}, {}},
        {"no hint", "SELECT 1; -- a comment", {}, {}},
    };
    for (const auto& c : cases) {
        const auto h = ParseTestHint(c.in);
        EXPECT_EQ("", h.parse_error) << c.name;
        EXPECT_EQ(c.server, h.server_errors) << c.name;
        EXPECT_EQ(c.client, h.client_errors) << c.name;
    }
}

TEST(ParseTestHint, Errors) {
    EXPECT_NE("", ParseTestHint("SELECT 1; -- { serverError NOT_A_REAL_ERROR_NAME }").parse_error);
    EXPECT_NE("", ParseTestHint("SELECT 1; -- { retry 3 until serverError 60 }").parse_error);
}

TEST(ParseTestHint, Matches) {
    const auto h = ParseTestHint("SELECT; -- { clientError SYNTAX_ERROR }");
    // Through clickhouse-cpp the server finds syntax errors.
    EXPECT_TRUE(h.Matches(62));
    EXPECT_FALSE(h.Matches(60));
    EXPECT_EQ("clientError SYNTAX_ERROR", h.Label());
}

TEST(ParseTestHint, Echo) {
    auto h = ParseTestHint("-- { echoOn }\nSELECT 1;");
    ASSERT_TRUE(h.echo.has_value());
    EXPECT_TRUE(*h.echo);
    h = ParseTestHint("SELECT 1; -- { echoOff }");
    ASSERT_TRUE(h.echo.has_value());
    EXPECT_FALSE(*h.echo);
}

TEST(ErrorCodes, Generated) {
    EXPECT_EQ(60, ErrorCodeByName("UNKNOWN_TABLE").value_or(-1));
    EXPECT_EQ(62, ErrorCodeByName("SYNTAX_ERROR").value_or(-1));
    EXPECT_FALSE(ErrorCodeByName("NOT_A_REAL_ERROR_NAME").has_value());
    EXPECT_EQ("UNKNOWN_TABLE", ErrorNameByCode(60));
    EXPECT_EQ("", ErrorNameByCode(-12345));
}

TEST(EchoesAfterError, SyntaxErrors) {
    // A syntax error of the statement itself: clickhouse-client does not parse
    // it, so it does not echo it.
    EXPECT_FALSE(EchoesAfterError(62, "DB::Exception: Syntax error: failed at position 11 (end of query): . Expected one of: token"));
    // A syntax error that a function finds in its argument: the statement
    // parsed, and clickhouse-client echoes it.
    EXPECT_TRUE(EchoesAfterError(62,
                                 "DB::Exception: Syntax error: failed at position 11 (end of query): . Expected one of: token: "
                                 "In scope SELECT formatQuery('SELECT 1 +'). (SYNTAX_ERROR)"));
    EXPECT_TRUE(EchoesAfterError(62,
                                 "DB::Exception: Syntax error: failed at position 1 (x): x. Expected one of: Query: "
                                 "While processing formatQuery('x')"));
    // Other errors: always echoed.
    EXPECT_TRUE(EchoesAfterError(60, "DB::Exception: Unknown table expression identifier 'x'"));
}
