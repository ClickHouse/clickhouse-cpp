#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace clickhouse_test_runner {

/// The `clickhouse-client` command line, as far as `tests/clickhouse-test`
/// uses it.
struct Options {
    std::string host = "localhost";
    int port         = 0;  ///< 0 when not given.
    std::string user = "default";
    std::string password;
    std::string database = "default";
    bool secure          = false;
    std::optional<std::string> query;
    bool help = false;

    /// The settings that go to the server with each query (see
    /// kServerSettings), in the order of the command line.
    std::vector<std::pair<std::string, std::string>> settings;
    /// Query parameters from `--param_<name>=<value>`.
    std::map<std::string, std::string> params;
};

/// Parses the command line (without the program name). Throws
/// std::invalid_argument for a bad value or a missing argument.
Options ParseArgs(const std::vector<std::string>& argv);

/// The command line with each `--password` value replaced by `***`, for the
/// log: `--password X` gives `--password ***`, `--password=X` gives
/// `--password=***`. It reads the options as ParseArgs does, so it finds the
/// value also after an option that takes `--password` as its own value
/// (`-u --password --password X`). A last `--password` stays as it is. Unlike
/// ParseArgs, it does not stop at `--`: a `--password` value after it is also
/// hidden.
std::vector<std::string> RedactPassword(const std::vector<std::string>& argv);

/// The settings of the command line that the shim sends to the server:
/// `log_comment`, `max_insert_threads`, `max_threads` and `send_logs_level`,
/// and the feature-tag overrides of `tests/clickhouse-test`: for a test with
/// the tag `no-async-insert` or `no-parallel-replicas`, the runner adds
/// `--async_insert 0` or `--enable_parallel_replicas 0`. These are not
/// randomized: the test needs the feature off, and `async_insert` is on by
/// default in current servers. `tests/clickhouse-test` also adds many
/// randomized settings. The shim drops them: they only change
/// performance-related paths in the server, and some of them are client-only.
bool IsServerSetting(const std::string& name);

extern const char kUsageText[];

}  // namespace clickhouse_test_runner
