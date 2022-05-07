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

template <typename NestedColumnType, typename ValuesContainer>
std::shared_ptr<ColumnArray> Create2DArray(const ValuesContainer& values) {
    auto result = std::make_shared<ColumnArray>(std::make_shared<ColumnArray>(std::make_shared<NestedColumnType>()));
    for (size_t i = 0; i < values.size(); ++i) {
        auto array_col = std::make_shared<ColumnArray>(std::make_shared<NestedColumnType>());
        for (size_t j = 0; j < values[i].size(); ++j)
            array_col->AppendAsColumn(std::make_shared<ColumnUInt64>(values[i][j]));

        result->AppendAsColumn(array_col);
    }

    return result;
}

template <typename NestedColumnType, typename ValuesContainer>
std::shared_ptr<ColumnArray> CreateArray(const ValuesContainer& values) {
    auto result = std::make_shared<ColumnArray>(std::make_shared<NestedColumnType>());
    for (size_t i = 0; i < values.size(); ++i) {
        result->AppendAsColumn(std::make_shared<NestedColumnType>(values[i]));
    }

    return result;
}

}

TEST(ColumnArray, Append) {
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

TEST(ColumnArray, ArrayOfDecimal) {
    auto column = std::make_shared<clickhouse::ColumnDecimal>(18, 10);
    auto array = std::make_shared<clickhouse::ColumnArray>(column->CloneEmpty());

    column->Append("1");
    column->Append("2");
    EXPECT_EQ(2u, column->Size());

    array->AppendAsColumn(column);
    ASSERT_EQ(1u, array->Size());
    EXPECT_EQ(2u, array->GetAsColumn(0)->Size());
}

TEST(ColumnArray, GetAsColumn) {
    // Verify that result of GetAsColumn
    // - is of proper type
    // - has expected length
    // - values match ones predefined ones

    const std::vector<std::vector<uint64_t>> values = {
        {1u, 2u, 3u},
        {4u, 5u, 6u, 7u, 8u, 9u},
        {0u},
        {},
        {13, 14}
    };

    auto array = CreateArray<ColumnUInt64>(values);
    ASSERT_EQ(values.size(), array->Size());

    for (size_t i = 0; i < values.size(); ++i) {
        auto row = array->GetAsColumn(i);
        std::shared_ptr<ColumnUInt64> typed_row;

        EXPECT_NO_THROW(typed_row = row->As<ColumnUInt64>());
        EXPECT_TRUE(CompareRecursive(values[i], *typed_row));
    }

    EXPECT_THROW(array->GetAsColumn(array->Size()), ValidationError);
    EXPECT_THROW(array->GetAsColumn(array->Size() + 1), ValidationError);
}

TEST(ColumnArray, Slice) {
    const std::vector<std::vector<uint64_t>> values = {
        {1u, 2u, 3u},
        {4u, 5u, 6u, 7u, 8u, 9u},
        {0u},
        {},
        {13, 14, 15}
    };

    std::shared_ptr<ColumnArray> untyped_array = CreateArray<ColumnUInt64>(values);

    for (size_t i = 0; i < values.size() - 1; ++i) {
        auto slice = untyped_array->Slice(i, 1)->AsStrict<ColumnArray>();
        EXPECT_EQ(1u, slice->Size());
        EXPECT_TRUE(CompareRecursive(values[i], *slice->GetAsColumnTyped<ColumnUInt64>(0)));
    }

    EXPECT_EQ(0u, untyped_array->Slice(0, 0)->Size());
    EXPECT_ANY_THROW(untyped_array->Slice(values.size(), 1));
    EXPECT_ANY_THROW(untyped_array->Slice(0, values.size() + 1));
}

TEST(ColumnArray, Slice_2D) {
    // Verify that ColumnArray::Slice on 2D Array produces a 2D Array of proper type, size and contents.
    // Also check that slices can be of any size.
    const std::vector<std::vector<std::vector<uint64_t>>> values = {
        {{1u, 2u}, {3u}},
        {{4u}, {5u, 6u, 7u}, {8u, 9u}, {}},
        {{0u}},
        {{}},
        {{13}, {14, 15}}
    };

    std::shared_ptr<ColumnArray> untyped_array = Create2DArray<ColumnUInt64>(values);
    for (size_t i = 0; i < values.size() - 1; ++i) {
        for (size_t slice_size = 0; slice_size < values.size() - i; ++slice_size) {
            auto slice = untyped_array->Slice(i, slice_size)->AsStrict<ColumnArray>();
            EXPECT_EQ(slice_size, slice->Size());

            for (size_t slice_row = 0; slice_row < slice_size; ++slice_row) {
                SCOPED_TRACE(::testing::Message() << "i: " << i << " slice_size:" << slice_size << " row:" << slice_row);
                auto val = slice->GetAsColumnTyped<ColumnArray>(slice_row);
                ASSERT_EQ(values[i + slice_row].size(), val->Size());

                for (size_t j = 0; j < values[i + slice_row].size(); ++j) {
                    ASSERT_TRUE(CompareRecursive(values[i + slice_row][j], *val->GetAsColumnTyped<ColumnUInt64>(j)));
                }
            }
        }
    }
}


template <typename ArrayTSpecialization, typename RowValuesContainer>
auto AppendRowAndTest(ArrayTSpecialization& array, const RowValuesContainer& values) {
    const size_t prev_size = array.Size();

    array.Append(values);
    EXPECT_EQ(prev_size + 1u, array.Size());

    EXPECT_TRUE(CompareRecursive(values, array.At(prev_size)));
    EXPECT_TRUE(CompareRecursive(values, array[prev_size]));

    // Check that both subscript and At() work properly.
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

TEST(ColumnArrayT, SimpleUInt64) {
    auto array = std::make_shared<clickhouse::ColumnArrayT<ColumnUInt64>>();
    array->Append({0, 1, 2});

    ASSERT_EQ(1u, array->Size());
    EXPECT_EQ(0u, array->At(0).At(0));
    EXPECT_EQ(1u, (*array)[0][1]);

    EXPECT_THROW(array->At(2), ValidationError);
    EXPECT_THROW(array->At(0).At(3), ValidationError);
    EXPECT_THROW((*array)[0].At(3), ValidationError);
}

TEST(ColumnArrayT, SimpleFixedString) {
    using namespace std::literals;
    auto array = std::make_shared<ColumnArrayT<ColumnFixedString>>(6);
    array->Append({"hello", "world"});

    // Additional \0 since strings are padded from right with zeros in FixedString(6).
    EXPECT_EQ("hello\0"sv, array->At(0).At(0));

    auto row = array->At(0);
    EXPECT_EQ("hello\0"sv, row.At(0));
    EXPECT_EQ(6u, row[0].length());
    EXPECT_EQ("hello", row[0].substr(0, 5));

    EXPECT_EQ("world\0"sv, (*array)[0][1]);
}

TEST(ColumnArrayT, SimpleUInt64_2D) {
    // Nested 2D-arrays are supported too:
    auto array = std::make_shared<ColumnArrayT<ColumnArrayT<ColumnUInt64>>>();
    array->Append(std::vector<std::vector<unsigned int>>{{0}, {1, 1}, {2, 2, 2}});

    ASSERT_EQ(1u, array->Size());
    EXPECT_EQ(0u, array->At(0).At(0).At(0));
    EXPECT_EQ(1u, (*array)[0][1][1]);

    EXPECT_THROW(array->At(2), ValidationError);
}

TEST(ColumnArrayT, UInt64) {
    // Check inserting\reading back data from clickhouse::ColumnArrayT<ColumnUInt64>

    const std::vector<std::vector<unsigned int>> values = {
        {1u, 2u, 3u},
        {4u, 5u, 6u, 7u, 8u, 9u},
        {0u},
        {},
        {13, 14}
    };
    CreateAndTestColumnArrayT<ColumnUInt64>(values);
}

TEST(ColumnArrayT, UInt64_2D) {
    // Check inserting\reading back data from 2D array: ColumnArrayT<ColumnArrayT<ColumnUInt64>>

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
        EXPECT_EQ(0u, array[3].size());

        // operator[] doesn't check bounds.
        // On empty rows attempt to access out-of-bound elements
        // would actually cause access to the elements of the next row.
        // hence non-0 value of `array[3][0].size()`,
        // it is effectively the same as `array[3 + 1][0].size()`
        EXPECT_EQ(2u, array[3][0].size());
        EXPECT_EQ(14u, array[3][0][1]);
        EXPECT_EQ(0u, array[3][1].size());
    }
}

TEST(ColumnArrayT, Wrap_UInt64) {
    // Check that ColumnArrayT can wrap a pre-existing ColumnArray.

    const std::vector<std::vector<uint64_t>> values = {
        {1u, 2u, 3u},
        {4u, 5u, 6u, 7u, 8u, 9u},
        {0u},
        {},
        {13, 14}
    };

    auto wrapped_array = ColumnArrayT<ColumnUInt64>::Wrap(CreateArray<ColumnUInt64>(values));
    const auto & array = *wrapped_array;

    EXPECT_TRUE(CompareRecursive(values, array));
}

TEST(ColumnArrayT, Wrap_UInt64_2D) {
    // Check that ColumnArrayT can wrap a pre-existing ColumnArray.

    const std::vector<std::vector<std::vector<uint64_t>>> values = {
        {{1u, 2u}, {3u}},
        {{4u}, {5u, 6u, 7u}, {8u, 9u}, {}},
        {{0u}},
        {{}},
        {{13}, {14, 15}}
    };

    auto wrapped_array = ColumnArrayT<ColumnArrayT<ColumnUInt64>>::Wrap(Create2DArray<ColumnUInt64>(values));
    const auto & array = *wrapped_array;

    EXPECT_TRUE(CompareRecursive(values, array));
}
