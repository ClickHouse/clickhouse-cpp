// clickhouse-test-runner imitates `clickhouse-client` well enough for the
// ClickHouse core test runner (`tests/clickhouse-test`) to drive it, and runs
// every query through clickhouse-cpp. It reads the values that clickhouse-cpp
// decoded and writes them as `TabSeparated` text, so the upstream `.reference`
// files check the decoding of the client.
//
// See README.md for how to run the upstream SQL tests with it.

#include <iostream>
#include <string>
#include <vector>

#include "shim.h"

int main(int argc, char* argv[]) {
    std::ios::sync_with_stdio(false);
    const std::vector<std::string> args(argv + 1, argv + argc);
    return clickhouse_test_runner::RunMain(args, std::cin, std::cout, std::cerr);
}
