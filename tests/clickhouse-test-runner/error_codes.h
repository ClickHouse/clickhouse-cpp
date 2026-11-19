#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace clickhouse_test_runner {

/// The code of a ClickHouse error name (`UNKNOWN_TABLE` -> 60), from
/// error_codes_generated.inc.
std::optional<int> ErrorCodeByName(std::string_view name);

/// The name of a ClickHouse error code, or an empty string.
std::string ErrorNameByCode(int code);

}  // namespace clickhouse_test_runner
