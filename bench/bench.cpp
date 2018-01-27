#include <benchmark/benchmark.h>

#include <clickhouse/client.h>

namespace clickhouse {

Client g_client(ClientOptions()
        .SetHost("localhost")
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
