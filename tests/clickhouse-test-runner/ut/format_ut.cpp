#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "format.h"

using namespace clickhouse_test_runner;

TEST(FindOutputFormat, Cases) {
    struct Case {
        std::string in, sql, format;
        bool supported;
    };
    const std::vector<Case> cases = {
        {"SELECT 1", "SELECT 1", "TabSeparated", true},
        {"SELECT 1 FORMAT TSV", "SELECT 1 ", "TSV", true},
        {"SELECT 1 FORMAT TSVWithNames SETTINGS max_threads = 1", "SELECT 1  SETTINGS max_threads = 1", "TSVWithNames", true},
        {"SELECT 1 FORMAT JSONEachRow", "SELECT 1 ", "JSONEachRow", false},
        {"SELECT 'FORMAT JSON'", "SELECT 'FORMAT JSON'", "TabSeparated", true},
        {"SELECT * FROM (SELECT 1 FORMAT JSON)", "SELECT * FROM (SELECT 1 FORMAT JSON)", "TabSeparated", true},
        {"SELECT t.format FROM t", "SELECT t.format FROM t", "TabSeparated", true},
        {"SELECT 1 FORMAT null", "SELECT 1 ", "null", true},
        {"SELECT 1 FORMAT TSVRaw", "SELECT 1 ", "TSVRaw", true},
        {"SELECT 1 FORMAT `Null`;", "SELECT 1 ;", "Null", true},
        {"SELECT 1 FORMAT Values", "SELECT 1 ", "Values", true},
        {"SELECT 1 FORMAT LineAsString", "SELECT 1 ", "LineAsString", true},
        {"SELECT 1 FORMAT CSV", "SELECT 1 ", "CSV", false},
        // The data of an INSERT, not an output format.
        {"INSERT INTO t FORMAT Values (1)", "INSERT INTO t FORMAT Values (1)", "TabSeparated", true},
        // FORMAT as an identifier, not the clause.
        {"SELECT format FROM t", "SELECT format FROM t", "TabSeparated", true},
        {"SELECT x AS format FROM t WHERE format = 1", "SELECT x AS format FROM t WHERE format = 1", "TabSeparated", true},
        {"SELECT 1 AS format FROM numbers(1)", "SELECT 1 AS format FROM numbers(1)", "TabSeparated", true},
        {"SELECT format FROM (SELECT 1 AS format)", "SELECT format FROM (SELECT 1 AS format)", "TabSeparated", true},
        {"SELECT 1 AS format", "SELECT 1 AS format", "TabSeparated", true},
        {"SELECT 1 AS format ORDER BY format DESC", "SELECT 1 AS format ORDER BY format DESC", "TabSeparated", true},
        {"SELECT 1 AS format ORDER BY format ASC", "SELECT 1 AS format ORDER BY format ASC", "TabSeparated", true},
        {"SELECT 1 AS format ORDER BY format DESC FORMAT TSV", "SELECT 1 AS format ORDER BY format DESC ", "TSV", true},
        {"SELECT 1 FORMAT TSV -- c\n;", "SELECT 1  -- c\n;", "TSV", true},
    };
    for (const auto& c : cases) {
        std::string sql;
        const auto f = FindOutputFormat(c.in, &sql);
        EXPECT_EQ(c.sql, sql) << c.in;
        EXPECT_EQ(c.format, f.name) << c.in;
        EXPECT_EQ(c.supported, f.supported) << c.in;
    }
}

TEST(FindOutputFormat, Flags) {
    auto f = FindOutputFormat("SELECT 1 FORMAT TabSeparatedWithNamesAndTypes");
    EXPECT_TRUE(f.names);
    EXPECT_TRUE(f.types);
    EXPECT_FALSE(f.raw);
    f = FindOutputFormat("SELECT 1 FORMAT RawWithNames");
    EXPECT_TRUE(f.raw);
    EXPECT_TRUE(f.names);
    EXPECT_FALSE(f.types);
    f = FindOutputFormat("SELECT 1 FORMAT Null");
    EXPECT_TRUE(f.discard);
    f = FindOutputFormat("SELECT 1 FORMAT values");
    EXPECT_TRUE(f.values);
    EXPECT_FALSE(f.raw);
    f = FindOutputFormat("SELECT 1 FORMAT LineAsString");
    EXPECT_TRUE(f.raw);
    EXPECT_FALSE(f.values);
}
