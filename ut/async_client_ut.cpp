#include <clickhouse/async_client.h>
#include <clickhouse/client.h>
#include <clickhouse/error_codes.h>

#include <ut/utils.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

#if defined(_win_)
#   include <process.h>
#else
#   include <unistd.h>
#endif

namespace {

using namespace clickhouse;

ClientOptions MakeClientOptions() {
    return ClientOptions()
        .SetHost(getEnvOrDefault("CLICKHOUSE_HOST", "localhost"))
        .SetPort(static_cast<std::uint16_t>(getEnvOrDefault<std::size_t>("CLICKHOUSE_PORT", "9000")))
        .SetUser(getEnvOrDefault("CLICKHOUSE_USER", "default"))
        .SetPassword(getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB", "default"));
}

AsyncClientOptions MakeAsyncClientOptions() {
    AsyncClientOptions options;
    options.host = getEnvOrDefault("CLICKHOUSE_HOST", "localhost");
    options.port = static_cast<std::uint16_t>(getEnvOrDefault<std::size_t>("CLICKHOUSE_PORT", "9000"));
    options.user = getEnvOrDefault("CLICKHOUSE_USER", "default");
    options.password = getEnvOrDefault("CLICKHOUSE_PASSWORD", "");
    options.database = getEnvOrDefault("CLICKHOUSE_DB", "default");
    return options;
}

std::string MakeUniqueTableName(std::string_view database) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
#if defined(_win_)
    const int pid = _getpid();
#else
    const int pid = getpid();
#endif
    std::ostringstream oss;
    oss << database << "."
        << "clickhouse_cpp_async_client_ut_"
        << pid << "_"
        << static_cast<unsigned long long>(now);
    return oss.str();
}

std::uint64_t SelectCount(Client& client, const std::string& table) {
    std::optional<std::uint64_t> count;
    client.Select("SELECT count() FROM " + table, [&](const Block& block) {
        if (block.GetRowCount() == 0) {
            return;
        }
        count = block[0]->As<ColumnUInt64>()->At(0);
    });
    if (!count.has_value()) {
        throw std::runtime_error("count() query returned no rows");
    }
    return *count;
}

}  // namespace

TEST(AsyncClientCase, Insert) {
    const auto client_options = MakeClientOptions();
    const auto async_options = MakeAsyncClientOptions();

    Client client(client_options);

    const auto table = MakeUniqueTableName(client_options.default_database);

    try {
        client.Execute("DROP TABLE IF EXISTS " + table);
        client.Execute("CREATE TABLE " + table + " (id UInt64, name String) ENGINE = Memory");
    } catch (const ServerError& e) {
        if (e.GetCode() == ErrorCodes::ACCESS_DENIED) {
            GTEST_SKIP() << e.what();
        }
        throw;
    }

    AsyncClient async(async_options);
    async.start_connect();

    const auto connect_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!async.connected() && std::chrono::steady_clock::now() < connect_deadline) {
        async.poll(std::chrono::steady_clock::now(), std::chrono::milliseconds(2));
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(async.connected());

    Block block;
    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    id->Append(2);

    auto name = std::make_shared<ColumnString>();
    name->Append("one");
    name->Append("two");

    block.AppendColumn("id", id);
    block.AppendColumn("name", name);

    ASSERT_EQ(async.enqueue_insert(table, block), EnqueueResult::queued);

    const auto insert_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::size_t completed = 0;
    while (completed < 1 && std::chrono::steady_clock::now() < insert_deadline) {
        const auto pr = async.poll(std::chrono::steady_clock::now(), std::chrono::milliseconds(2));
        completed += pr.requests_completed;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    ASSERT_EQ(completed, 1u);
    EXPECT_EQ(SelectCount(client, table), 2u);

    try {
        client.Execute("DROP TABLE IF EXISTS " + table);
    } catch (const ServerError& e) {
        if (e.GetCode() != ErrorCodes::ACCESS_DENIED) {
            throw;
        }
    }
}
