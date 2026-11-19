#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace clickhouse_test_runner {

/// The test hints of one statement, as `clickhouse-client` parses them (see
/// `src/Client/TestHint.cpp` in ClickHouse):
///
///     SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
///     SELECT 1 / 0;      -- { serverError 153, 6 }
///     -- { echoOn }
///
/// An error hint counts only when it is not in a leading comment. An `echo`
/// hint counts in any comment.
struct TestHint {
    std::vector<int> server_errors;
    std::vector<int> client_errors;
    /// Set by `echo`, `echoOn` (true) or `echoOff` (false).
    std::optional<bool> echo;
    /// Not empty when a hint is malformed or names an unknown error.
    std::string parse_error;

    bool ExpectsError() const { return !server_errors.empty() || !client_errors.empty(); }

    /// Tells if the error code satisfies the hint. Through clickhouse-cpp the
    /// server finds every error, also the syntax errors that
    /// `clickhouse-client` finds itself, so a server error can satisfy a
    /// `clientError` hint too.
    bool Matches(int code) const;

    /// `serverError UNKNOWN_TABLE`, as the messages show it.
    std::string Label() const;
};

/// Parses the hints of a statement text (the statement with its same-line
/// trailing comment).
TestHint ParseTestHint(std::string_view full);

}  // namespace clickhouse_test_runner
