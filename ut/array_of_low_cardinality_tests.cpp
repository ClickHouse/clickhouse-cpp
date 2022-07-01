#include <gtest/gtest.h>
#include <algorithm>
#include <iterator>
#include <vector>

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/lowcardinality.h>
#include "clickhouse/block.h"
#include "clickhouse/client.h"
#include "utils.h"
#include "clickhouse/base/buffer.h"
#include "clickhouse/base/output.h"

namespace
{
using namespace clickhouse;
}

std::shared_ptr<ColumnArray> buildTestColumn(const std::vector<std::vector<std::string>>& rows) {
    auto arrayColumn = std::make_shared<ColumnArray>(std::make_shared<ColumnLowCardinalityT<ColumnString>>());

    for (const auto& row : rows) {
        auto column = std::make_shared<ColumnLowCardinalityT<ColumnString>>();

        for (const auto& string : row) {
            column->Append(string);
        }

        arrayColumn->AppendAsColumn(column);
    }

    return arrayColumn;
}

TEST(ArrayOfLowCardinality, Serialization) {
    const auto inputColumn = buildTestColumn({
        { "aa", "bb" },
        { "cc" }
    });

    // The serialization data was extracted from a successful insert.
    // When compared to what Clickhouse/NativeWriter does for the same fields, the only differences are the index type and indexes.
    // Since we are setting a different index type in clickhouse-cpp, it's expected to have different indexes.
    const std::vector<uint8_t> expectedSerialization {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x61, 0x61,
        0x02, 0x62, 0x62, 0x02, 0x63, 0x63, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x00
    };

    Buffer buf;

    BufferOutput output(&buf);
    inputColumn->Save(&output);
    output.Flush();

    ASSERT_EQ(expectedSerialization, buf);
}

TEST(ArrayOfLowCardinality, InsertAndQuery) {

    const auto localHostEndpoint = ClientOptions()
                                       .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
                                       .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
                                       .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
                                       .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
                                       .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));

    Client client(ClientOptions(localHostEndpoint)
                      .SetPingBeforeQuery(true));

    const auto testData = std::vector<std::vector<std::string>> {
        { "aa", "bb" },
        {},
        { "cc" },
        {}
    };

    auto column = buildTestColumn(testData);

    Block block;
    block.AppendColumn("arr", column);

    client.Execute("DROP TEMPORARY TABLE IF EXISTS array_lc");
    client.Execute("CREATE TEMPORARY TABLE IF NOT EXISTS array_lc (arr Array(LowCardinality(String))) ENGINE = Memory");
    client.Insert("array_lc", block);

    client.Select("SELECT * FROM array_lc", [&](const Block& bl) {
        for (size_t c = 0; c < bl.GetRowCount(); ++c) {
          auto col = bl[0]->As<ColumnArray>()->GetAsColumn(c);
          for (size_t i = 0; i < col->Size(); ++i) {
              if (auto string_column = col->As<ColumnString>()) {
                  const auto string = string_column->At(i);
                  ASSERT_EQ(testData[c][i], string);
              } else if (auto lc_string_column = col->As<ColumnLowCardinalityT<ColumnString>>()) {
                  const auto string = lc_string_column->At(i);
                  ASSERT_EQ(testData[c][i], string);
              } else {
                  FAIL() << "Unexpected column type: " << col->Type()->GetName();
              }
          }
        }
    });
}
