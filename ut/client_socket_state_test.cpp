#include <clickhouse/client.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>
#include <clickhouse/query.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <system_error>
#include <thread>

#include "utils.h"

namespace {

using namespace clickhouse;

constexpr uint64_t kMarker = 12648430;

ClientOptions MakeClientOptions() {
    return ClientOptions()
        .SetHost(getEnvOrDefault("CLICKHOUSE_HOST", "localhost"))
        .SetPort(getEnvOrDefault<size_t>("CLICKHOUSE_PORT", "9000"))
        .SetUser(getEnvOrDefault("CLICKHOUSE_USER", "default"))
        .SetPassword(getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB", "default"))
        .SetConnectionRecvTimeout(std::chrono::seconds(5));
}

bool IsUnsupportedCompressionError(const ServerException& error) {
    // Verified with ClickHouse 25.12. Older releases may reject or ignore compression NONE
    switch (error.GetCode()) {
        case ErrorCodes::INVALID_SETTING_VALUE:
        case ErrorCodes::UNKNOWN_COMPRESSION_METHOD:
        case ErrorCodes::UNKNOWN_SETTING:
        case ErrorCodes::SUPPORT_IS_DISABLED:
            return true;
        default:
            return false;
    }
}

bool IsUnsupportedJsonError(const ServerException& error) {
    // The native JSON type was introduced in ClickHouse 24.8. Earlier releases do
    // not have this type.
    switch (error.GetCode()) {
        case ErrorCodes::NOT_IMPLEMENTED:
        case ErrorCodes::UNKNOWN_SETTING:
        case ErrorCodes::UNKNOWN_TYPE:
        case ErrorCodes::SUPPORT_IS_DISABLED:
            return true;
        default:
            return false;
    }
}

bool SelectReturnsMarker(Client& client) {
    bool valid  = true;
    size_t rows = 0;

    try {
        client.Select("SELECT CAST(12648430 AS UInt64) AS marker", [&](const Block& block) {
            if (block.GetRowCount() == 0) {
                return;
            }
            if (block.GetColumnCount() != 1) {
                valid = false;
                return;
            }

            const auto column = block[0]->As<ColumnUInt64>();
            if (!column) {
                valid = false;
                return;
            }

            for (size_t i = 0; i < column->Size(); ++i) {
                valid = valid && column->At(i) == kMarker;
                ++rows;
            }
        });
    } catch (...) {
        return false;
    }

    return valid && rows == 1;
}

void ExpectSocketIsHealthy(Client& client) {
    // Stale EndOfStream may end the probe without throwing, so verify its exact result.
    EXPECT_TRUE(SelectReturnsMarker(client));

    // ASSERT_NO_THROW(client.ResetConnection());
    // EXPECT_TRUE(SelectReturnsMarker(client));
}

class CallbackError final : public std::exception {};

class ClientSocketStateTest : public testing::Test {
protected:
    void SetUp() override { client_ = std::make_unique<Client>(MakeClientOptions()); }

    std::unique_ptr<Client> client_;
};

TEST_F(ClientSocketStateTest, UnsupportedResultTypeBreaksSocketState) {
    EXPECT_THROW(client_->Execute("SELECT sumState(number) FROM numbers(10)"), UnimplementedError);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, TotalsBreakSocketState) {
    EXPECT_THROW(client_->Execute("SELECT number % 2 AS key, count() "
                                  "FROM numbers(10) GROUP BY key WITH TOTALS"),
                 UnimplementedError);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ExtremesBreakSocketState) {
    Query query("SELECT number FROM numbers(10)");
    query.SetSetting("extremes", {"1"});

    EXPECT_THROW(client_->Execute(query), UnimplementedError);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ThrowingOnDataBreaksSocketState) {
    bool callback_called = false;
    Query query(
        "SELECT number FROM system.numbers "
        "LIMIT 100000 SETTINGS max_block_size = 100");
    query.OnData([&](const Block& block) {
        if (block.GetRowCount() > 0) {
            callback_called = true;
            throw CallbackError{};
        }
    });

    EXPECT_THROW(client_->Execute(query), CallbackError);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ThrowingOnDataCancelableBreaksSocketState) {
    bool callback_called = false;
    Query query(
        "SELECT number FROM system.numbers "
        "LIMIT 100000 SETTINGS max_block_size = 100");
    query.OnDataCancelable([&](const Block& block) {
        if (block.GetRowCount() > 0) {
            callback_called = true;
            throw CallbackError{};
        }
        return true;
    });

    EXPECT_THROW(client_->Execute(query), CallbackError);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ThrowingOnProfileBreaksSocketState) {
    bool callback_called = false;
    Query query("SELECT * FROM system.numbers LIMIT 10");
    query.OnProfile([&](const Profile&) {
        callback_called = true;
        throw CallbackError{};
    });

    EXPECT_THROW(client_->Execute(query), CallbackError);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ThrowingOnProgressBreaksSocketState) {
    client_->Execute("CREATE TEMPORARY TABLE socket_state_progress (value String) ENGINE = Memory");

    bool callback_called = false;
    Query query("INSERT INTO socket_state_progress VALUES ('Foo'), ('Bar')");
    query.OnProgress([&](const Progress&) {
        callback_called = true;
        throw CallbackError{};
    });

    EXPECT_THROW(client_->Execute(query), CallbackError);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ThrowingOnServerLogBreaksSocketState) {
    client_->Execute("CREATE TEMPORARY TABLE socket_state_server_log (value String) ENGINE = Memory");

    bool callback_called = false;
    Query query("INSERT INTO socket_state_server_log VALUES ('Foo'), ('Bar')");
    query.SetSetting("send_logs_level", {"trace"});
    query.OnServerLog([&](const Block&) -> bool {
        callback_called = true;
        throw CallbackError{};
    });

    EXPECT_THROW(client_->Execute(query), CallbackError);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ThrowingOnProfileEventsBreaksSocketState) {
    constexpr uint64_t kMinRevisionWithIncrementalProfileEvents = 54451;
    if (client_->GetServerInfo().revision < kMinRevisionWithIncrementalProfileEvents) {
        GTEST_SKIP() << "Server does not support incremental profile events";
    }

    client_->Execute("CREATE TEMPORARY TABLE socket_state_profile_events (value String) ENGINE = Memory");
    client_->Execute("INSERT INTO socket_state_profile_events VALUES ('Foo'), ('Bar')");

    bool callback_called = false;
    Query query("SELECT * FROM socket_state_profile_events");
    query.OnProfileEvents([&](const Block&) -> bool {
        callback_called = true;
        throw CallbackError{};
    });

    EXPECT_THROW(client_->Execute(query), CallbackError);
    EXPECT_TRUE(callback_called);
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

TEST_F(ClientSocketStateTest, ReceiveTimeoutBreaksSocketState) {
    ClientOptions options = MakeClientOptions();
    options.SetConnectionRecvTimeout(std::chrono::milliseconds(500));
    Client client(options);

    Query query("SELECT sleep(1)");
    query.SetSetting("interactive_delay", {"10000000"});

    EXPECT_THROW(client.Execute(query), std::system_error);
    EXPECT_FALSE(client.IsSelecting());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ExpectSocketIsHealthy(client);
}

TEST_F(ClientSocketStateTest, NoneNetworkCompressionBreaksSocketState) {
    ClientOptions options = MakeClientOptions();
    options.SetCompressionMethod(CompressionMethod::LZ4);
    Client client(options);

    Query query("SELECT number FROM numbers(10)");
    query.SetSetting("network_compression_method", {"NONE"});

    bool compression_error = false;
    try {
        client.Execute(query);
    } catch (const CompressionError&) {
        compression_error = true;
    } catch (const ServerException& error) {
        if (IsUnsupportedCompressionError(error)) {
            GTEST_SKIP() << "Server does not support NONE network compression: " << error.what();
        }
        throw;
    }

    if (!compression_error) {
        GTEST_SKIP() << "Server did not emit NONE-compressed frames";
    }
    EXPECT_FALSE(client.IsSelecting());

    ExpectSocketIsHealthy(client);
}

TEST_F(ClientSocketStateTest, NativeJsonSerializationBreaksSocketState) {
    try {
        Query create("CREATE TEMPORARY TABLE socket_state_json (value JSON) ENGINE = Memory");
        create.SetSetting("allow_experimental_json_type", {"1"});
        client_->Execute(create);

        Query insert("INSERT INTO socket_state_json VALUES ('{\"key\": 1}')");
        insert.SetSetting("allow_experimental_json_type", {"1"});
        client_->Execute(insert);
    } catch (const ServerException& error) {
        if (IsUnsupportedJsonError(error)) {
            GTEST_SKIP() << "Server does not support the JSON test setup: " << error.what();
        }
        throw;
    }

    Query query("SELECT value FROM socket_state_json");
    query.SetSetting("allow_experimental_json_type", {"1"});

    bool protocol_error = false;
    try {
        client_->Execute(query);
    } catch (const ProtocolError&) {
        protocol_error = true;
    }

    if (!protocol_error) {
        GTEST_SKIP() << "Server did not emit native JSON serialization";
    }
    EXPECT_FALSE(client_->IsSelecting());

    ExpectSocketIsHealthy(*client_);
}

}  // namespace
