#pragma once

#include <string>
#include <string_view>

namespace clickhouse_test_runner {

/// Collects troubleshooting lines for one invocation and appends them to the
/// file named by CLICKHOUSE_CLIENT_CLI_LOG. Without that variable, it does
/// nothing. The lines are written in one write, so the lines of parallel
/// invocations do not mix.
class InvocationLog {
public:
    InvocationLog();
    ~InvocationLog();

    InvocationLog(const InvocationLog&)            = delete;
    InvocationLog& operator=(const InvocationLog&) = delete;

    void Add(std::string_view key, std::string_view value);

    /// Appends the lines to the log file. A log that cannot be written must
    /// not change the result of a test, so a write error is ignored; the
    /// missing lines in the log show it.
    void Flush();

private:
    std::string path_;
    std::string lines_;
};

}  // namespace clickhouse_test_runner
