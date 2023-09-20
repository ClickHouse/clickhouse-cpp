#include "roundtrip_column.h"

#include <clickhouse/client.h>
#include <clickhouse/block.h>

#include <gtest/gtest.h>
#include <type_traits>
#include "clickhouse/columns/numeric.h"

namespace {
using namespace clickhouse;

template <typename T>
std::vector<T> GenerateConsecutiveNumbers(size_t count, T start = 0)
{
    std::vector<T> result;
    result.reserve(count);

    T value = start;
    for (size_t i = 0; i < count; ++i, ++value)
    {
        result.push_back(value);
    }

    return result;
}

}


ColumnRef RoundtripColumnValues(Client& client, ColumnRef expected) {
    // Create a temporary table with a corresponding data column
    // INSERT values from `expected`
    // SELECT and collect all values from block into `result` column
    auto result = expected->CloneEmpty();

    const std::string type_name = result->GetType().GetName();
    client.Execute("DROP TEMPORARY TABLE IF EXISTS temporary_roundtrip_table;");
    // id column is to have the same order of rows on SELECT
    client.Execute("CREATE TEMPORARY TABLE IF NOT EXISTS temporary_roundtrip_table (id UInt32, col " + type_name + ");");
    {
        Block block;
        block.AppendColumn("col", expected);
        block.AppendColumn("id", std::make_shared<ColumnUInt32>(GenerateConsecutiveNumbers<uint32_t>(expected->Size())));
        block.RefreshRowCount();
        client.Insert("temporary_roundtrip_table", block);
    }

    client.Select("SELECT col FROM temporary_roundtrip_table ORDER BY id", [&result](const Block& b) {
        if (b.GetRowCount() == 0)
            return;

        ASSERT_EQ(1u, b.GetColumnCount());
        result->Append(b[0]);
    });

    EXPECT_EQ(expected->GetType(), result->GetType());
    EXPECT_EQ(expected->Size(), result->Size());

    return result;
}
