#pragma once

#include <string>
#include <string_view>

namespace clickhouse_test_runner {

/// How the shim writes a result set.
struct OutputFormat {
    std::string name = "TabSeparated";
    bool names       = false;  ///< A header line with the column names.
    bool types       = false;  ///< A header line with the column types.
    bool raw         = false;  ///< Top-level strings are not escaped.
    bool discard     = false;  ///< The `Null` format.
    bool values      = false;  ///< The `Values` format: `(a,b),(c,d)`, values as in an array, no newline.
    bool supported   = true;
};

/// Finds a top-level `FORMAT <name>` clause of a statement and returns the
/// format. clickhouse-cpp reads every result as `Native` blocks, and the shim
/// writes them itself, so only the `TabSeparated` family, `LineAsString` (the
/// server writes it as `TabSeparatedRaw`), `Values` and `Null` are supported. `FORMAT` is the clause only at the end of a query (before an
/// optional `SETTINGS`), and only when the word after it is not a SQL
/// keyword: `SELECT format FROM t` and `ORDER BY format DESC` have a column
/// named `format`.
///
/// When `without_clause` is not null, it receives the statement without the
/// clause.
OutputFormat FindOutputFormat(std::string_view sql, std::string* without_clause = nullptr);

}  // namespace clickhouse_test_runner
