#include <clickhouse/columns/array.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>

#include <gtest/gtest.h>
#include "utils.h"

#include <string_view>
#include <sstream>
#include <vector>

namespace {
using namespace clickhouse;
}

TEST(ColumnsCase, ArrayAppend) {
    auto arr1 = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
    auto arr2 = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    arr1->AppendAsColumn(id);

    id->Append(3);
    arr2->AppendAsColumn(id);

    arr1->Append(arr2);

    auto col = arr1->GetAsColumn(1);

    ASSERT_EQ(arr1->Size(), 2u);
    ASSERT_EQ(col->As<ColumnUInt64>()->At(0), 1u);
    ASSERT_EQ(col->As<ColumnUInt64>()->At(1), 3u);
}

TEST(ColumnsCase, ArrayOfDecimal) {
    auto column = std::make_shared<clickhouse::ColumnDecimal>(18, 10);
    auto array = std::make_shared<clickhouse::ColumnArray>(column->CloneEmpty());

    column->Append("1");
    column->Append("2");
    EXPECT_EQ(2u, column->Size());

    array->AppendAsColumn(column);
    ASSERT_EQ(1u, array->Size());
    EXPECT_EQ(2u, array->GetAsColumn(0)->Size());
}

template <typename ArrayTSpecialization, typename RowValuesContainer>
auto AppendRowAndTest(ArrayTSpecialization& array, const RowValuesContainer& values) {
//    SCOPED_TRACE(PrintContainer{values});
    const size_t prev_size = array.Size();

    array.Append(values);
    EXPECT_EQ(prev_size + 1u, array.Size());

    EXPECT_TRUE(CompareRecursive(values, array.At(prev_size)));
    EXPECT_TRUE(CompareRecursive(values, array[prev_size]));

    // Check that both subscrip and At() work properly.
    const auto & new_row = array.At(prev_size);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_TRUE(CompareRecursive(*(values.begin() + i), new_row[i]))
                << " at pos: " << i;
        EXPECT_TRUE(CompareRecursive(*(values.begin() + i), new_row.At(i)))
                << " at pos: " << i;
    }
    EXPECT_THROW(new_row.At(new_row.size() + 1), clickhouse::ValidationError);
};

template <typename NestedColumnType, typename AllValuesContainer>
auto CreateAndTestColumnArrayT(const AllValuesContainer& all_values) {
    auto array = std::make_shared<clickhouse::ColumnArrayT<NestedColumnType>>();

    for (const auto & row : all_values) {
        EXPECT_NO_FATAL_FAILURE(AppendRowAndTest(*array, row));
    }
    EXPECT_TRUE(CompareRecursive(all_values, *array));
    EXPECT_THROW(array->At(array->Size() + 1), clickhouse::ValidationError);

    return array;
}

TEST(ColumnsCase, ArrayTUint64) {
    // Check inserting\reading back data from clickhouse::ColumnArrayT<ColumnUInt64>

    const std::vector<std::vector<unsigned int>> values = {
        {1u, 2u, 3u},
        {4u, 5u, 6u, 7u, 8u, 9u},
        {0u},
        {},
        {13, 14}
    };
    auto array_ptr = CreateAndTestColumnArrayT<ColumnUInt64>(values);
    const auto & array = *array_ptr;

    // Make sure that chaining of brackets works.
    EXPECT_EQ(1u, array[0][0]);
    EXPECT_EQ(2u, array[0][1]);
    EXPECT_EQ(3u, array[0][2]);

    // empty row
    EXPECT_EQ(0u, array[3].size());

    EXPECT_EQ(2u, array[4].size());
    EXPECT_EQ(13u, array[4][0]);
    EXPECT_EQ(14u, array[4][1]);

    // Make sure that At() throws an exception on nested items
    EXPECT_THROW(array.At(5), clickhouse::ValidationError);
    EXPECT_THROW(array[3].At(0), clickhouse::ValidationError);
    EXPECT_THROW(array[4].At(3), clickhouse::ValidationError);
}

TEST(ColumnsCase, ArrayTOfArrayTUint64) {
    // Check inserting\reading back data from 2D array: clickhouse::ColumnArrayT<ColumnArrayT<ColumnUInt64>>

    const std::vector<std::vector<std::vector<unsigned int>>> values = {
        {{1u, 2u, 3u}, {4u, 5u, 6u}},
        {{4u, 5u, 6u}, {7u, 8u, 9u}, {10u, 11u}},
        {{}, {}},
        {},
        {{13, 14}, {}}
    };

    auto array_ptr = CreateAndTestColumnArrayT<ColumnArrayT<ColumnUInt64>>(values);
    const auto & array = *array_ptr;

    {
        const auto row = array[0];
        EXPECT_EQ(1u, row[0][0]);
        EXPECT_EQ(2u, row[0][1]);
        EXPECT_EQ(6u, row[1][2]);
    }

    {
        EXPECT_EQ(8u,  array[1][1][1]);
        EXPECT_EQ(11u, array[1][2][1]);
    }

    {
        EXPECT_EQ(2u, array[2].size());
        EXPECT_EQ(0u, array[2][0].size());
        EXPECT_EQ(0u, array[2][1].size());
    }

    {
        EXPECT_EQ(0u, array[3].size());

        // [] doesn't check bounds.
        // On empty rows attempt to access out-of-bound elements
        // would actually cause access to the elements of the next row.
        // hence non-0 value of `array[3][0].size()`,
        // it is effectively the same as `array[3 + 1][0].size()`
        EXPECT_EQ(2u, array[3][0].size());
        EXPECT_EQ(14u, array[3][0][1]);
        EXPECT_EQ(0u, array[3][1].size());
    }

    {
        EXPECT_EQ(14u, array[4][0][1]);
        EXPECT_EQ(0u, array[4][1].size());
    }
}

TEST(ColumnsCase, ArrayTWrap) {
    // TODO(nemkov): wrap 2D array
    // Check that ColumnArrayT can wrap a pre-existing ColumnArray,
    // pre-existing data is kept intact and new rows can be inserted.

    const std::vector<std::vector<uint64_t>> values = {
        {1u, 2u, 3u},
        {4u, 5u, 6u, 7u, 8u, 9u},
        {0u},
        {},
        {13, 14}
    };

    std::shared_ptr<ColumnArray> untyped_array = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
    for (size_t i = 0; i < values.size(); ++i) {
        untyped_array->AppendAsColumn(std::make_shared<ColumnUInt64>(values[i]));
    }

    auto wrapped_array = ColumnArrayT<ColumnUInt64>::Wrap(std::move(*untyped_array));
    // Upon wrapping, internals of columns are "stolen" and the column shouldn't be used anymore.
//    EXPECT_EQ(0u, untyped_array->Size());

    const auto & array = *wrapped_array;

    EXPECT_TRUE(CompareRecursive(values, array));
}

TEST(ColumnsCase, ArrayTArrayTWrap) {
    // TODO(nemkov): wrap 2D array
    // Check that ColumnArrayT can wrap a pre-existing ColumnArray,
    // pre-existing data is kept intact and new rows can be inserted.

    const std::vector<std::vector<std::vector<uint64_t>>> values = {
//        {{1u, 2u}, {3u}},
//        {{4u}, {5u, 6u, 7u}, {8u, 9u}, {}},
//        {{0u}},
        {{}},
//        {{13}, {14, 15}}
    };

    std::shared_ptr<ColumnArray> untyped_array = std::make_shared<ColumnArray>(std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>()));
    for (size_t i = 0; i < values.size(); ++i) {
        auto array_col = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
        for (size_t j = 0; j < values[i].size(); ++j) {
            const auto & v = values[i][j];
            SCOPED_TRACE(::testing::Message() << "i: " << i << " j:" << j << " " << PrintContainer{v});
            array_col->AppendAsColumn(std::make_shared<ColumnUInt64>(v));
        }

        untyped_array->AppendAsColumn(array_col);
    }

    auto wrapped_array = ColumnArrayT<ColumnArrayT<ColumnUInt64>>::Wrap(std::move(*untyped_array));
    const auto & array = *wrapped_array;

    EXPECT_TRUE(CompareRecursive(values, array));
}

TEST(ColumnsCase, ArrayTSimpleUint64) {
    auto array = std::make_shared<clickhouse::ColumnArrayT<ColumnUInt64>>();
    array->Append({0, 1, 2});

    EXPECT_EQ(0u, array->At(0).At(0)); // 0
    EXPECT_EQ(1u, (*array)[0][1]);     // 1
}

TEST(ColumnsCase, ArrayTSimpleFixedString) {
    using namespace std::literals;
    auto array = std::make_shared<clickhouse::ColumnArrayT<ColumnFixedString>>(6);
    array->Append({"hello", "world"});

    // Additional \0 since strings are padded from right with zeros in FixedString(6).
    EXPECT_EQ("hello\0"sv, array->At(0).At(0));

    auto row = array->At(0);
    EXPECT_EQ("hello\0"sv, row.At(0));
    EXPECT_EQ(6u, row[0].length());
    EXPECT_EQ("hello", row[0].substr(0, 5));

    EXPECT_EQ("world\0"sv, (*array)[0][1]);
}

TEST(ColumnsCase, ArrayTSimpleArrayOfUint64) {
    // Nested 2D-arrays are supported too:
    auto array = std::make_shared<clickhouse::ColumnArrayT<clickhouse::ColumnArrayT<ColumnUInt64>>>();
    array->Append(std::vector<std::vector<unsigned int>>{{0}, {1, 1}, {2, 2, 2}});

    EXPECT_EQ(0u, array->At(0).At(0).At(0)); // 0
    EXPECT_EQ(1u, (*array)[0][1][1]);        // 1
}
