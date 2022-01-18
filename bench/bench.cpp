#include <benchmark/benchmark.h>

#include <clickhouse/client.h>

namespace clickhouse {

Client g_client(ClientOptions()
        .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
        .SetPort( std::stoi(getEnvOrDefault("CLICKHOUSE_PORT",     "9000")))
        .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
        .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"))
        .SetPingBeforeQuery(false));

static void SelectNumber(benchmark::State& state) {
    while (state.KeepRunning()) {
        g_client.Select("SELECT number, number, number FROM system.numbers LIMIT 1000",
            [](const Block& block) { block.GetRowCount(); }
        );
    }
}
BENCHMARK(SelectNumber);

static void SelectNumberMoreColumns(benchmark::State& state) {
    // Mainly test performance on type name parsing.
    while (state.KeepRunning()) {
        g_client.Select("SELECT "
                "number, number, number, number, number, number, number, number, number, number "
                "FROM system.numbers LIMIT 100",
            [](const Block& block) { block.GetRowCount(); }
        );
    }
}
BENCHMARK(SelectNumberMoreColumns);

}

BENCHMARK_MAIN();
