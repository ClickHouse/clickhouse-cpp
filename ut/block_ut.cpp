#include <clickhouse/client.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/types/types.h>

#include "clickhouse/columns/column.h"
#include "clickhouse/columns/lowcardinality.h"
#include "gtest/gtest-message.h"

#include "ut/utils_comparison.h"
#include "utils.h"

#include <gtest/gtest.h>
#include <memory>

namespace {
using namespace clickhouse;

Block MakeBlock(std::vector<std::pair<std::string, ColumnRef>> columns) {
    Block result;

    const size_t number_of_rows = columns.size() ? columns[0].second->Size() : 0;
    size_t i = 0;
    for (const auto & name_and_col : columns) {
        EXPECT_EQ(number_of_rows, name_and_col.second->Size())
        << "Column #" << i <<  " " << name_and_col.first << " has incorrect number of rows";

        result.AppendColumn(name_and_col.first, name_and_col.second);

        ++i;
    }

    result.RefreshRowCount();
    EXPECT_EQ(number_of_rows, result.GetRowCount());

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
        EXPECT_EQ(0u, c.Column()->Size());

        ++i;
    }
}

TEST(BlockTest, Reserve) {
    // Test that Block::Reserve reserves space in all columns (uncheckable now),
    // without changing column instances, names, and previously stored rows.

    auto block = MakeBlock({
        {"foo", std::make_shared<ColumnUInt8>(std::vector<uint8_t>{1, 2, 3, 4, 5})},
        {"bar", std::make_shared<ColumnString>(std::vector<std::string>{"1", "2", "3", "4", "5"})},
        {"quix", std::make_shared<ColumnLowCardinalityT<ColumnString>>(std::vector<std::string>{"1", "2", "3", "4", "5"})},
    });

    const size_t initial_rows_count = block.GetRowCount();

    std::vector<std::tuple<std::string, Column*, ColumnRef>> expected_columns_description;
    for (const auto & c : block) {
        expected_columns_description.emplace_back(
            c.Name(),
            c.Column().get(),                        // reference to the actual object
            c.Column()->Slice(0, c.Column()->Size()) // reference to the values
        );
    }

    block.Reserve(1000); // 1000 is arbitrary value

    // Block must same number of rows as before Reserve
    EXPECT_EQ(initial_rows_count, block.GetRowCount());

    size_t i = 0;
    for (const auto & c : block) {
        const auto & [expected_name, expected_column, expected_values] = expected_columns_description[i];
        SCOPED_TRACE(testing::Message("col #") << c.ColumnIndex() << " \"" << c.Name() << "\"");

        // MUST have same column name
        EXPECT_EQ(expected_name, c.Name());

        // MUST be same column object
        EXPECT_EQ(expected_column, c.Column().get());

        // column MUST have the same values
        EXPECT_TRUE(CompareRecursive(*expected_values, *c.Column()));

        ++i;
    }
}
