#include <clickhouse/client.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/types/types.h>

#include "clickhouse/columns/column.h"
#include "gtest/gtest-message.h"
#include "readonly_client_test.h"
#include "connection_failed_client_test.h"
#include "utils.h"

#include <gtest/gtest.h>

namespace {
using namespace clickhouse;

Block MakeBlock(std::vector<std::pair<std::string, ColumnRef>> columns) {
    Block result;

    for (const auto & name_and_col : columns) {
        result.AppendColumn(name_and_col.first, name_and_col.second);
    }

    result.RefreshRowCount();
    return result;
}

}

TEST(BlockTest, Iterator) {
    const auto block = MakeBlock({
        {"foo", std::make_shared<ColumnUInt8>(std::vector<uint8_t>{1, 2, 3, 4, 5})},
        {"bar", std::make_shared<ColumnString>(std::vector<std::string>{"1", "2", "3", "4", "5"})},
    });
    const char* names[] = {"foo", "bar"};

    ASSERT_EQ(2u, block.GetColumnCount());
    ASSERT_EQ(5u, block.GetRowCount());

    size_t col_index = 0;
    Block::Iterator i(block);
    while (i.IsValid()) {

        const auto& name_and_col = i;
        ASSERT_EQ(col_index, name_and_col.ColumnIndex());
        ASSERT_EQ(block[col_index].get(), name_and_col.Column().get());
        ASSERT_EQ(names[col_index], name_and_col.Name());

        i.Next();
        ++col_index;
    }
}

TEST(BlockTest, RangeBasedForLoop) {
    const auto block = MakeBlock({
        {"foo", std::make_shared<ColumnUInt8>(std::vector<uint8_t>{1, 2, 3, 4, 5})},
        {"bar", std::make_shared<ColumnString>(std::vector<std::string>{"1", "2", "3", "4", "5"})},
    });
    const char* names[] = {"foo", "bar"};

    ASSERT_EQ(2u, block.GetColumnCount());
    ASSERT_EQ(5u, block.GetRowCount());

    size_t col_index = 0;
    for (const auto & name_and_col : block) {
        ASSERT_EQ(col_index, name_and_col.ColumnIndex());
        ASSERT_EQ(block[col_index].get(), name_and_col.Column().get());
        ASSERT_EQ(names[col_index], name_and_col.Name());
        ++col_index;
    }
}

TEST(BlockTest, Iterators) {
    Block block;
    // Empty block, all iterators point to 'end'
    ASSERT_EQ(block.begin(), block.cbegin());
    ASSERT_EQ(block.end(), block.cend());
    ASSERT_EQ(block.begin(), block.end());

    block = MakeBlock({
        {"foo", std::make_shared<ColumnUInt8>(std::vector<uint8_t>{1, 2, 3, 4, 5})},
        {"bar", std::make_shared<ColumnString>(std::vector<std::string>{"1", "2", "3", "4", "5"})},
    });

    // Non-empty block
    ASSERT_EQ(block.begin(), block.cbegin());
    ASSERT_EQ(block.end(), block.cend());
    ASSERT_NE(block.begin(), block.end());
    ASSERT_NE(block.cbegin(), block.cend());
}

TEST(BlockTest, Clear) {
    // Test that Block::Clear removes all rows from all of the columns,
    // without changing column instances, types, names, etc.

    auto block = MakeBlock({
        {"foo", std::make_shared<ColumnUInt8>(std::vector<uint8_t>{1, 2, 3, 4, 5})},
        {"bar", std::make_shared<ColumnString>(std::vector<std::string>{"1", "2", "3", "4", "5"})},
    });

    std::vector<std::tuple<std::string, Column*>> expected_columns_description;
    for (const auto & c : block) {
        expected_columns_description.emplace_back(c.Name(), c.Column().get());
    }

    block.Clear();

    // Block must report empty after being cleared
    EXPECT_EQ(0u, block.GetRowCount());

    size_t i = 0;
    for (const auto & c : block) {
        const auto & [expected_name, expected_column] = expected_columns_description[i];
        SCOPED_TRACE(testing::Message("col #") << c.ColumnIndex() << " \"" << c.Name() << "\"");

        // MUST be same column object
        EXPECT_EQ(expected_column, c.Column().get());

        // MUST have same column name
        EXPECT_EQ(expected_name, c.Name());

        // column MUST be empty
        EXPECT_EQ(0u, c.Column()->Size())
            << c.ColumnIndex() << " : " << c.Name();

        ++i;
    }
}


