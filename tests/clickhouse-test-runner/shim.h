#pragma once

#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace clickhouse_test_runner {

/// The program: imitates `clickhouse-client` (and `clickhouse
/// extract-from-config`) well enough for `tests/clickhouse-test` to drive it.
/// `argv` has no program name. Returns the exit code.
int RunMain(const std::vector<std::string>& argv, std::istream& in, std::ostream& out, std::ostream& err);

}  // namespace clickhouse_test_runner
