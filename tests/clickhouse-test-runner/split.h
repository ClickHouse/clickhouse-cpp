#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "hint.h"

namespace clickhouse_test_runner {

/// One query of a multi-query script, split the way `clickhouse-client`
/// splits it (see `ClientBase::executeMultiQuery` in ClickHouse).
struct Statement {
    /// The text to send to the server: from the first non-whitespace
    /// character (leading comments included) to the end of the query, with
    /// the semicolon.
    std::string sql;
    /// `sql` plus the same-line trailing comment. The client reads the test
    /// hints from this text and prints it for `echo`.
    std::string full;
    TestHint hint;
    /// Tells if the statement is printed before it runs (the `echo` hints).
    bool echo = false;
};

/// Splits a script into statements, as `clickhouse-client` does:
///   - The `-- Tags:` header (the first line and the `--` lines after it) is
///     not part of any statement.
///   - A statement ends at a top-level `;`. A comment on the same line after
///     the `;` belongs to that statement; this is where test hints are.
///   - The inline data of `INSERT ... FORMAT <name>` (other than `Values`)
///     ends at an empty line, not at `;`.
///   - Comment-only text after the last statement is not a statement.
std::vector<Statement> SplitStatements(std::string_view text);

/// Mirrors `getTestTagsLength`: the length of the leading `-- Tags: ...` line
/// and the `--` comment lines right after it.
size_t TestTagsLength(std::string_view text);

}  // namespace clickhouse_test_runner
