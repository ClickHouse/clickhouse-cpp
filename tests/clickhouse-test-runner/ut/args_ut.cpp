#include <gtest/gtest.h>

#include <sstream>
#include <stdexcept>

#include "args.h"
#include "runner.h"
#include "shim.h"

using namespace clickhouse_test_runner;

using Settings = std::vector<std::pair<std::string, std::string>>;
using Argv     = std::vector<std::string>;

TEST(ParseArgs, SQLTestCommandLine) {
    // The command line that tests/clickhouse-test uses for a .sql test.
    const auto opts = ParseArgs({
        "client",
        "--host=127.0.0.1",
        "--port=9000",
        "--no-secure",
        "--database=test_abc",
        "--send_logs_level=warning",
        "--multiquery",
        "--log_comment",
        "00001_select_1.sql-test_abc",
        "--max_block_size=1",
        "--allow_repeated_settings",
        "--client_logs_file=/tmp/x-fatal",
        "--param_CLICKHOUSE_DATABASE=test_abc",
    });
    EXPECT_EQ("127.0.0.1", opts.host);
    EXPECT_EQ(9000, opts.port);
    EXPECT_EQ("test_abc", opts.database);
    EXPECT_FALSE(opts.secure);
    EXPECT_FALSE(opts.query.has_value());
    EXPECT_EQ((Settings{{"send_logs_level", "warning"}, {"log_comment", "00001_select_1.sql-test_abc"}}), opts.settings);
    EXPECT_EQ((std::map<std::string, std::string>{{"CLICKHOUSE_DATABASE", "test_abc"}}), opts.params);
}

TEST(ParseArgs, FeatureTagOverrides) {
    // A test with the tags no-async-insert and no-parallel-replicas: the runner
    // turns the features off on the command line. The client-only settings of
    // the MergeTree codec injection stay out.
    const auto opts = ParseArgs({
        "--log_comment",
        "x.sql-test_abc",
        "--allow_merge_tree_settings",
        "--default_compression_codec",
        "LZ4",
        "--allow_repeated_settings",
        "--enable_parallel_replicas",
        "0",
        "--async_insert",
        "0",
    });
    EXPECT_EQ((Settings{{"log_comment", "x.sql-test_abc"}, {"enable_parallel_replicas", "0"}, {"async_insert", "0"}}), opts.settings);
}

TEST(ParseArgs, ShortOptions) {
    const auto opts = ParseArgs({"-h", "db", "-u", "me", "-d", "x", "-q", "SELECT 1", "-n", "-s"});
    EXPECT_EQ("db", opts.host);
    EXPECT_EQ("me", opts.user);
    EXPECT_EQ("x", opts.database);
    ASSERT_TRUE(opts.query.has_value());
    EXPECT_EQ("SELECT 1", *opts.query);
    EXPECT_TRUE(opts.secure);
}

TEST(ParseArgs, SettingValues) {
    const auto opts = ParseArgs({"--max_threads", "-1", "--max-insert-threads", "2", "--query=SELECT 1"});
    EXPECT_EQ((Settings{{"max_threads", "-1"}, {"max_insert_threads", "2"}}), opts.settings);
}

TEST(ParseArgs, Errors) {
    EXPECT_THROW(ParseArgs({"--port=abc"}), std::invalid_argument);
    EXPECT_THROW(ParseArgs({"--query"}), std::invalid_argument);
}

TEST(RedactPassword, HidesThePasswordOnly) {
    EXPECT_EQ((Argv{"--user", "u", "--password", "***", "--password=***", "--query", "SELECT 1", "--password"}),
              RedactPassword({"--user", "u", "--password", "secret", "--password=secret2", "--query", "SELECT 1", "--password"}));

    struct Case {
        Argv argv;
        std::string password;  ///< The password that ParseArgs reads.
        Argv want;
    };
    const std::vector<Case> cases = {
        // An option that takes a value can take `--password` as its value. A
        // setting (`--max_threads`) never does.
        {{"-u", "--password", "--password", "secret"}, "secret", {"-u", "--password", "--password", "***"}},
        {{"--host", "--password", "--password", "secret"}, "secret", {"--host", "--password", "--password", "***"}},
        {{"--max_threads", "--password", "secret"}, "secret", {"--max_threads", "--password", "***"}},
        {{"--max_threads", "-1", "--password", "secret"}, "secret", {"--max_threads", "-1", "--password", "***"}},
        // Contrast: the value of another option stays.
        {{"-u", "--password", "x"}, "", {"-u", "--password", "x"}},
        {{"--host", "--password=x"}, "", {"--host", "--password=x"}},
    };
    for (const auto& c : cases) {
        EXPECT_EQ(c.password, ParseArgs(c.argv).password) << testing::PrintToString(c.argv);
        EXPECT_EQ(c.want, RedactPassword(c.argv)) << testing::PrintToString(c.argv);
    }
}

TEST(RunMain, ExtractFromConfig) {
    std::istringstream in;
    std::ostringstream out, err;
    EXPECT_EQ(0, RunMain({"extract-from-config", "--try", "--config", "x.xml", "--key", "listen_host"}, in, out, err));
    EXPECT_EQ("127.0.0.1\n", out.str());

    out.str("");
    EXPECT_EQ(0, RunMain({"extract-from-config", "--key=tcp_port"}, in, out, err));
    EXPECT_EQ("", out.str());
    EXPECT_EQ("", err.str());
}

TEST(RunMain, NoQuery) {
    std::istringstream in("  \n");
    std::ostringstream out, err;
    EXPECT_EQ(1, RunMain({}, in, out, err));
    EXPECT_NE(std::string::npos, err.str().find("no query"));
}

TEST(ParseSetAssignments, Values) {
    EXPECT_EQ((Settings{{"dialect", "kusto"}, {"max_threads", "1"}, {"x", "(1, 2)"}}),
              ParseSetAssignments("SET dialect = 'kusto', max_threads=1, x = (1, 2);"));
}

TEST(ParseSetAssignments, Comments) {
    EXPECT_EQ((Settings{{"max_block_size", "100"}, {"max_threads", "1"}}),
              ParseSetAssignments("SET\n  max_block_size = 100,\n  -- one thread\n  max_threads = 1;"));
    EXPECT_EQ((Settings{{"max_block_size", "100"}, {"max_insert_threads", "1"}}),
              ParseSetAssignments("SET\n  -- don't use more threads\n  max_block_size = 100,\n  max_insert_threads = 1;"));
    EXPECT_EQ((Settings{{"format_tsv_null_representation", "NULL"}}),
              ParseSetAssignments("SET format_tsv_null_representation = 'NULL' /* text of NULL */;"));
    EXPECT_EQ((Settings{{"dialect", "clickhouse"}}), ParseSetAssignments("SET dialect = 'clickhouse' -- back to SQL"));
    EXPECT_EQ((Settings{{"max_threads", "1"}}), ParseSetAssignments("SET max_threads = 1 # a hash comment\n;"));
}

TEST(ParseSetAssignments, CommentMarkersInStrings) {
    EXPECT_EQ((Settings{{"format_csv_delimiter", "#"}, {"x", "-- not a comment"}, {"y", "/* kept */"}}),
              ParseSetAssignments("SET format_csv_delimiter = '#', x = '-- not a comment', y = '/* kept */';"));
}

TEST(ParseSetAssignments, Heredocs) {
    EXPECT_EQ((Settings{{"x", "$$-- not a comment$$"}, {"max_threads", "1"}}),
              ParseSetAssignments("SET x = $$-- not a comment$$, max_threads = 1;"));
    EXPECT_EQ((Settings{{"x", "$tag$a, /* b */ c$tag$"}, {"y", "2"}}), ParseSetAssignments("SET x = $tag$a, /* b */ c$tag$, y = 2;"));
}
