#include <gtest/gtest.h>
#include <clickhouse/columns/string.h>
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/client.h"
#include "utils.h"
#include "clickhouse/base/wire_format.h"
#include <clickhouse/base/output.h>

namespace
{
using namespace clickhouse;
}

static const auto localHostEndpoint = ClientOptions()
                                   .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
                                   .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
                                   .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
                                   .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
                                   .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));


ColumnRef buildTestColumn(const std::vector<std::string>& rowsData, const std::vector<uint8_t>& nulls) {
    auto stringColumn = std::make_shared<ColumnString>(rowsData);
    auto nullsColumn = std::make_shared<ColumnUInt8>(nulls);
    auto lowCardinalityColumn = std::make_shared<ColumnLowCardinality>(
        std::make_shared<ColumnNullable>(stringColumn, nullsColumn)
    );

    return lowCardinalityColumn;
}

void createTable(Client& client) {
    client.Execute("DROP TEMPORARY TABLE IF EXISTS lc_of_nullable");
    client.Execute("CREATE TEMPORARY TABLE IF NOT EXISTS lc_of_nullable (words LowCardinality(Nullable(String))) ENGINE = Memory");
}

TEST(LowCardinalityOfNullable, InsertAndQuery) {
    const auto rowsData = std::vector<std::string> {
        "eminem",
        "",
        "tupac",
        "shady",
        "fifty",
        "dre",
        "",
        "cube"
    };

    const auto nulls = std::vector<uint8_t> {
        false, false, true, false, true, true, false, false
    };

    auto column = buildTestColumn(rowsData, nulls);

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
                             .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
                             .SetPingBeforeQuery(true));

    createTable(client);

    client.Insert("lc_of_nullable", block);

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        for (size_t row = 0; row < bl.GetRowCount(); row++) {
            auto lc_col = bl[0]->As<ColumnLowCardinality>();
            auto item = lc_col->GetItem(row);

            if (nulls[row]) {
                ASSERT_EQ(Type::Code::Void, item.type);
            } else {
                ASSERT_EQ(rowsData[row], item.get<std::string_view>());
            }
        }
    });
}

TEST(LowCardinalityOfNullable, InsertAndQueryOneRow) {
    const auto rowsData = std::vector<std::string> {
        "eminem"
    };

    const auto nulls = std::vector<uint8_t> {
        false
    };

    auto column = buildTestColumn(rowsData, nulls);

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
                             .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
                             .SetPingBeforeQuery(true));

    createTable(client);

    client.Insert("lc_of_nullable", block);

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        for (size_t row = 0; row < bl.GetRowCount(); row++) {
            auto lc_col = bl[0]->As<ColumnLowCardinality>();
            auto item = lc_col->GetItem(row);

            if (nulls[row]) {
                ASSERT_EQ(Type::Code::Void, item.type);
            } else {
                ASSERT_EQ(rowsData[row], item.get<std::string_view>());
            }
        }
    });
}


TEST(LowCardinalityOfNullable, InsertAndQueryEmpty) {
    auto column = buildTestColumn({}, {});

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
            .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
            .SetPingBeforeQuery(true));

    createTable(client);

    EXPECT_NO_THROW(client.Insert("lc_of_nullable", block));

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        ASSERT_EQ(bl.GetRowCount(), 0u);
    });
}

TEST(LowCardinalityOfNullable, ThrowOnBackwardsCompatibleLCColumn) {
    auto column = buildTestColumn({}, {});

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
            .SetPingBeforeQuery(true)
            .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(true));

    createTable(client);

    EXPECT_THROW(client.Insert("lc_of_nullable", block), UnimplementedError);

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        ASSERT_EQ(bl.GetRowCount(), 0u);
    });
}
