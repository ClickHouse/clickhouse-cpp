#include "roundtrip_column.h"

#include <clickhouse/client.h>
#include <clickhouse/block.h>

#include <gtest/gtest.h>

namespace {
using namespace clickhouse;
}

ColumnRef RoundtripColumnValues(Client& client, ColumnRef expected) {
    // Create a temporary table with a single column
    // insert values from `expected`
    // select and aggregate all values from block into `result` column
    auto result = expected->CloneEmpty();

    const std::string type_name = result->GetType().GetName();
    client.Execute("DROP TEMPORARY TABLE IF EXISTS temporary_roundtrip_table;");
    client.Execute("CREATE TEMPORARY TABLE IF NOT EXISTS temporary_roundtrip_table (col " + type_name + ");");
    {
        Block block;
        block.AppendColumn("col", expected);
        block.RefreshRowCount();
        client.Insert("temporary_roundtrip_table", block);
    }

    client.Select("SELECT col FROM temporary_roundtrip_table", [&result](const Block& b) {
        if (b.GetRowCount() == 0)
            return;

        ASSERT_EQ(1u, b.GetColumnCount());
        result->Append(b[0]);
    });

    EXPECT_EQ(expected->GetType(), result->GetType());
    EXPECT_EQ(expected->Size(), result->Size());

    return result;
}
