#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/string.h>

#include <gtest/gtest.h>
#include "clickhouse/exceptions.h"
#include "gtest/gtest-message.h"
#include "utils.h"
#include "value_generators.h"

#include <limits>
#include <string_view>
#include <sstream>
#include <vector>
#include <random>

namespace {

using namespace clickhouse;
using namespace std::literals::string_view_literals;

static const auto LOWCARDINALITY_STRING_FOOBAR_10_ITEMS_BINARY =
    "\x01\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00"
    "\x09\x00\x00\x00\x00\x00\x00\x00\x00\x06\x46\x6f\x6f\x42\x61\x72"
    "\x01\x31\x01\x32\x03\x46\x6f\x6f\x01\x34\x03\x42\x61\x72\x01\x37"
    "\x01\x38\x0a\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06"
    "\x04\x07\x08\x04"sv;
}


TEST(ColumnsCase, ColumnLowCardinalityString_Append_and_Read) {
    const size_t items_count = 11;
    ColumnLowCardinalityT<ColumnString> col;
    for (const auto & item : GenerateVector(items_count, &FooBarGenerator)) {
        col.Append(item);
    }

    ASSERT_EQ(col.Size(), items_count);
    ASSERT_EQ(col.GetDictionarySize(), 8u + 1); // 8 unique items from sequence + 1 null-item

    for (size_t i = 0; i < items_count; ++i) {
        ASSERT_EQ(col.At(i), FooBarGenerator(i)) << " at pos: " << i;
        ASSERT_EQ(col[i], FooBarGenerator(i)) << " at pos: " << i;
    }
}

TEST(ColumnsCase, ColumnLowCardinalityString_Clear_and_Append) {
    const size_t items_count = 11;
    ColumnLowCardinalityT<ColumnString> col;
    for (const auto & item : GenerateVector(items_count, &FooBarGenerator))
    {
        col.Append(item);
    }

    col.Clear();
    ASSERT_EQ(col.Size(), 0u);
    ASSERT_EQ(col.GetDictionarySize(), 1u); // null-item

    for (const auto & item : GenerateVector(items_count, &FooBarGenerator))
    {
        col.Append(item);
    }

    ASSERT_EQ(col.Size(), items_count);
    ASSERT_EQ(col.GetDictionarySize(), 8u + 1); // 8 unique items from sequence + 1 null-item
}

TEST(ColumnsCase, ColumnLowCardinalityString_Load) {
    const size_t items_count = 10;
    ColumnLowCardinalityT<ColumnString> col;

    const auto & data = LOWCARDINALITY_STRING_FOOBAR_10_ITEMS_BINARY;
    ArrayInput buffer(data.data(), data.size());

    ASSERT_TRUE(col.Load(&buffer, items_count));

    for (size_t i = 0; i < items_count; ++i) {
        EXPECT_EQ(col.At(i), FooBarGenerator(i)) << " at pos: " << i;
    }
}

// This is temporary disabled since we are not 100% compatitable with ClickHouse
// on how we serailize LC columns, but we check interoperability in other tests (see client_ut.cpp)
TEST(ColumnsCase, DISABLED_ColumnLowCardinalityString_Save) {
    const size_t items_count = 10;
    ColumnLowCardinalityT<ColumnString> col;
    for (const auto & item : GenerateVector(items_count, &FooBarGenerator)) {
        col.Append(item);
    }

    ArrayOutput output(0, 0);

    const size_t expected_output_size = LOWCARDINALITY_STRING_FOOBAR_10_ITEMS_BINARY.size();
    // Enough space to account for possible overflow from both right and left sides.
    std::string buffer(expected_output_size * 10, '\0');// = {'\0'};
    const char margin_content[sizeof(buffer)] = {'\0'};

    const size_t left_margin_size = 10;
    const size_t right_margin_size = sizeof(buffer) - left_margin_size - expected_output_size;

            // Since overflow from left side is less likely to happen, leave only tiny margin there.
    auto write_pos = buffer.data() + left_margin_size;
    const auto left_margin = buffer.data();
    const auto right_margin = write_pos + expected_output_size;

    output.Reset(write_pos, expected_output_size);

    EXPECT_NO_THROW(col.Save(&output));

            // Left margin should be blank
    EXPECT_EQ(std::string_view(margin_content, left_margin_size), std::string_view(left_margin, left_margin_size));
    // Right margin should be blank too
    EXPECT_EQ(std::string_view(margin_content, right_margin_size), std::string_view(right_margin, right_margin_size));

            // TODO: right now LC columns do not write indexes in the most compact way possible, so binary representation is a bit different
            // (there might be other inconsistances too)
    EXPECT_EQ(LOWCARDINALITY_STRING_FOOBAR_10_ITEMS_BINARY, std::string_view(write_pos, expected_output_size));
}

TEST(ColumnsCase, ColumnLowCardinalityString_SaveAndLoad) {
    // Verify that we can load binary representation back
    ColumnLowCardinalityT<ColumnString> col;

    const auto items = GenerateVector(10, &FooBarGenerator);
    for (const auto & item : items) {
        col.Append(item);
    }

    char buffer[256] = {'\0'}; // about 3 times more space than needed for this set of values.
    {
        ArrayOutput output(buffer, sizeof(buffer));
        EXPECT_NO_THROW(col.Save(&output));
    }

    col.Clear();

    {
        // Load the data back
        ArrayInput input(buffer, sizeof(buffer));
        EXPECT_TRUE(col.Load(&input, items.size()));
    }

    for (size_t i = 0; i < items.size(); ++i) {
        EXPECT_EQ(col.At(i), items[i]) << " at pos: " << i;
    }
}

TEST(ColumnsCase, ColumnLowCardinalityString_WithEmptyString_1) {
    // Verify that when empty string is added to a LC column it can be retrieved back as empty string.
    ColumnLowCardinalityT<ColumnString> col;
    const auto values = GenerateVector(10, AlternateGenerators<std::string>(SameValueGenerator<std::string>(""), FooBarGenerator));
    for (const auto & item : values) {
        col.Append(item);
    }

    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(values[i], col.At(i)) << " at pos: " << i;
    }
}

TEST(ColumnsCase, ColumnLowCardinalityString_WithEmptyString_2) {
    // Verify that when empty string is added to a LC column it can be retrieved back as empty string.
    // (Ver2): Make sure that outcome doesn't depend if empty values are on odd positions
    ColumnLowCardinalityT<ColumnString> col;
    const auto values = GenerateVector(10, AlternateGenerators<std::string>(FooBarGenerator, SameValueGenerator<std::string>("")));
    for (const auto & item : values) {
        col.Append(item);
    }

    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(values[i], col.At(i)) << " at pos: " << i;
    }
}

TEST(ColumnsCase, ColumnLowCardinalityString_WithEmptyString_3) {
    // When we have many leading empty strings and some non-empty values.
    ColumnLowCardinalityT<ColumnString> col;
    const auto values = ConcatSequences(GenerateVector(100, SameValueGenerator<std::string>("")), GenerateVector(5, FooBarGenerator));
    for (const auto & item : values) {
        col.Append(item);
    }

    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(values[i], col.At(i)) << " at pos: " << i;
    }
}

TEST(ColumnLowCardinalityString, WithSizeEstimation) {
    const ColumnString::EstimatedValueSize value_size_estimations[] = {
        ColumnString::EstimatedValueSize::TINY,
        ColumnString::EstimatedValueSize::SMALL,
        ColumnString::EstimatedValueSize::MEDIUM,
        ColumnString::EstimatedValueSize::LARGE,

        //        ColumnString::EstimatedValueSize(0),
        ColumnString::EstimatedValueSize(1),
        ColumnString::EstimatedValueSize(300),
        ColumnString::EstimatedValueSize(10'000),
    };

    auto values = MakeStrings();

            // How many times to append items from values to column.
    for (size_t count = 512; count <= 1024; count *= 2)
    {
        std::cerr << "\nNumber of values: " << values.size() * count << std::endl;
        for (ColumnString::EstimatedValueSize estimation : value_size_estimations) {
            SCOPED_TRACE(::testing::Message("with estimation: ") << estimation);
            std::cerr << "Estimation " << estimation << std::endl;

            auto col = std::make_shared<ColumnLowCardinalityT<ColumnString>>(estimation);

            dumpMemoryUsage("After constructing with estimation", col);
//ASSERT_NO_FATAL_FAILURE
            col->Reserve(values.size() * count);
            dumpMemoryUsage("After Reserve()", col);

            size_t i = 0;
            for (size_t j = 0; j < count; ++j)
            {
                for (const auto & v : values) {
                    col->Append(v);

                    EXPECT_EQ(i + 1, col->Size());
                    EXPECT_EQ(v, col->At(i));

                    ++i;
                }
            }

            dumpMemoryUsage("After appending all values", col);
        }
    }
}
