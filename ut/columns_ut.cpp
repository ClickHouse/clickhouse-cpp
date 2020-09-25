#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>

#include <contrib/gtest/gtest.h>
#include "utils.h"

#include <string_view>


namespace {

using namespace clickhouse;
using namespace std::literals::string_view_literals;

static std::vector<uint32_t> MakeNumbers() {
    return std::vector<uint32_t>
        {1, 2, 3, 7, 11, 13, 17, 19, 23, 29, 31};
}

static std::vector<uint8_t> MakeBools() {
    return std::vector<uint8_t>
        {1, 0, 0, 0, 1, 1, 0, 1, 1, 1, 0};
}

static std::vector<std::string> MakeFixedStrings() {
    return std::vector<std::string>
        {"aaa", "bbb", "ccc", "ddd"};
}

static std::vector<std::string> MakeStrings() {
    return std::vector<std::string>
        {"a", "ab", "abc", "abcd"};
}

static std::vector<uint64_t> MakeUUIDs() {
    return std::vector<uint64_t>
        {0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu,
         0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu,
         0x3507213c178649f9llu, 0x9faf035d662f60aellu};
}

static const auto LOWCARDINALITY_STRING_FOOBAR_10_ITEMS_BINARY =
        "\x01\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00"
        "\x09\x00\x00\x00\x00\x00\x00\x00\x00\x06\x46\x6f\x6f\x42\x61\x72"
        "\x01\x31\x01\x32\x03\x46\x6f\x6f\x01\x34\x03\x42\x61\x72\x01\x37"
        "\x01\x38\x0a\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06"
        "\x04\x07\x08\x04"sv;

template <typename Generator>
auto GenerateVector(size_t items, Generator && gen) {
    std::vector<std::result_of_t<Generator(size_t)>> result;
    result.reserve(items);
    for (size_t i = 0; i < items; ++i) {
        result.push_back(std::move(gen(i)));
    }

    return result;
}

std::string FooBarSeq(size_t i) {
    std::string result;
    if (i % 3 == 0)
        result += "Foo";
    if (i % 5 == 0)
        result += "Bar";
    if (result.empty())
        result = std::to_string(i);

    return result;
}

template <typename T, typename U = T>
auto SameValueSeq(const U & value) {
    return [&value](size_t) -> T {
        return value;
    };
}

template <typename ResultType, typename Generator1, typename Generator2>
auto AlternateGenerators(Generator1 && gen1, Generator2 && gen2) {
    return [&gen1, &gen2](size_t i) -> ResultType {
        if (i % 2 == 0)
            return gen1(i/2);
        else
            return gen2(i/2);
    };
}

template <typename T>
std::vector<T> ConcatSequences(std::vector<T> && vec1, std::vector<T> && vec2)
{
    std::vector<T> result(vec1);

    result.reserve(vec1.size() + vec2.size());
    result.insert(result.end(), vec2.begin(), vec2.end());

    return result;
}

static std::vector<Int64> MakeDateTime64s() {
    static const auto seconds_multiplier = 1'000'000;
    static const auto year = 86400ull * 365 * seconds_multiplier; // ~approx, but this doesn't matter here.

    // Approximatelly +/- 200 years around epoch (and value of epoch itself)
    // with non zero seconds and sub-seconds.
    // Please note there are values outside of DateTime (32-bit) range that might
    // not have correct string representation in CH yet,
    // but still are supported as Int64 values.
    return GenerateVector(200,
        [] (size_t i )-> Int64 {
            return (i - 100) * year * 2 + (i * 10) * seconds_multiplier + i;
        });
}

}

// TODO: add tests for ColumnDecimal.

TEST(ColumnsCase, NumericInit) {
    auto col = std::make_shared<ColumnUInt32>(MakeNumbers());

    ASSERT_EQ(col->Size(), 11u);
    ASSERT_EQ(col->At(3),   7u);
    ASSERT_EQ(col->At(10), 31u);

    auto sun = std::make_shared<ColumnUInt32>(MakeNumbers());
}

TEST(ColumnsCase, NumericSlice) {
    auto col = std::make_shared<ColumnUInt32>(MakeNumbers());
    auto sub = col->Slice(3, 3)->As<ColumnUInt32>();

    ASSERT_EQ(sub->Size(), 3u);
    ASSERT_EQ(sub->At(0),  7u);
    ASSERT_EQ(sub->At(2), 13u);
}


TEST(ColumnsCase, FixedStringInit) {
    auto col = std::make_shared<ColumnFixedString>(3);
    for (const auto& s : MakeFixedStrings()) {
        col->Append(s);
    }

    ASSERT_EQ(col->Size(), 4u);
    ASSERT_EQ(col->At(1), "bbb");
    ASSERT_EQ(col->At(3), "ddd");
}

TEST(ColumnsCase, StringInit) {
    auto col = std::make_shared<ColumnString>(MakeStrings());

    ASSERT_EQ(col->Size(), 4u);
    ASSERT_EQ(col->At(1), "ab");
    ASSERT_EQ(col->At(3), "abcd");
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
    //ASSERT_EQ(col->As<ColumnUInt64>()->At(0), 1u);
    //ASSERT_EQ(col->As<ColumnUInt64>()->At(1), 3u);
}

TEST(ColumnsCase, DateAppend) {
    auto col1 = std::make_shared<ColumnDate>();
    auto col2 = std::make_shared<ColumnDate>();
    auto now  = std::time(nullptr);

    col1->Append(now);
    col2->Append(col1);

    ASSERT_EQ(col2->Size(), 1u);
    ASSERT_EQ(col2->At(0), (now / 86400) * 86400);
}

TEST(ColumnsCase, DateTime64_0) {
    auto column = std::make_shared<ColumnDateTime64>(0ul);

    ASSERT_EQ(Type::DateTime64, column->Type()->GetCode());
    ASSERT_EQ("DateTime64(0)", column->Type()->GetName());
    ASSERT_EQ(0u, column->GetPrecision());
    ASSERT_EQ(0u, column->Size());
}

TEST(ColumnsCase, DateTime64_6) {
    auto column = std::make_shared<ColumnDateTime64>(6ul);

    ASSERT_EQ(Type::DateTime64, column->Type()->GetCode());
    ASSERT_EQ("DateTime64(6)", column->Type()->GetName());
    ASSERT_EQ(6u, column->GetPrecision());
    ASSERT_EQ(0u, column->Size());
}

TEST(ColumnsCase, DateTime64_Append_At) {
    auto column = std::make_shared<ColumnDateTime64>(6ul);

    const auto data = MakeDateTime64s();
    for (const auto & v : data) {
        column->Append(v);
    }

    ASSERT_EQ(data.size(), column->Size());
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_EQ(data[i], column->At(i));
    }
}

TEST(ColumnsCase, DateTime64_Clear) {
    auto column = std::make_shared<ColumnDateTime64>(6ul);

    // Clearing empty column doesn't crash and produces expected result
    ASSERT_NO_THROW(column->Clear());
    ASSERT_EQ(0u, column->Size());

    const auto data = MakeDateTime64s();
    for (const auto & v : data) {
        column->Append(v);
    }

    ASSERT_NO_THROW(column->Clear());
    ASSERT_EQ(0u, column->Size());
}

TEST(ColumnsCase, DateTime64_Swap) {
    auto column = std::make_shared<ColumnDateTime64>(6ul);

    const auto data = MakeDateTime64s();
    for (const auto & v : data) {
        column->Append(v);
    }

    auto column2 = std::make_shared<ColumnDateTime64>(6ul);
    const auto single_dt64_value = 1'234'567'890'123'456'789ll;
    column2->Append(single_dt64_value);
    column->Swap(*column2);

    // Validate that all items were transferred to column2.
    ASSERT_EQ(1u, column->Size());
    EXPECT_EQ(single_dt64_value, column->At(0));

    ASSERT_EQ(data.size(), column2->Size());
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_EQ(data[i], column2->At(i));
    }
}

TEST(ColumnsCase, DateTime64_Slice) {
    auto column = std::make_shared<ColumnDateTime64>(6ul);

    {
        // Empty slice on empty column
        auto slice = column->Slice(0, 0)->As<ColumnDateTime64>();
        ASSERT_EQ(0u, slice->Size());
        ASSERT_EQ(column->GetPrecision(), slice->GetPrecision());
    }

    const auto data = MakeDateTime64s();
    const size_t size = data.size();
    ASSERT_GT(size, 4u); // so the partial slice below has half of the elements of the column

    for (const auto & v : data) {
        column->Append(v);
    }

    {
        // Empty slice on non-empty column
        auto slice = column->Slice(0, 0)->As<ColumnDateTime64>();
        ASSERT_EQ(0u, slice->Size());
        ASSERT_EQ(column->GetPrecision(), slice->GetPrecision());
    }

    {
        // Full-slice on non-empty column
        auto slice = column->Slice(0, size)->As<ColumnDateTime64>();
        ASSERT_EQ(column->Size(), slice->Size());
        ASSERT_EQ(column->GetPrecision(), slice->GetPrecision());

        for (size_t i = 0; i < data.size(); ++i) {
            ASSERT_EQ(data[i], slice->At(i));
        }
    }

    {
        const size_t offset = size / 4;
        const size_t count = size / 2;
        // Partial slice on non-empty column
        auto slice = column->Slice(offset, count)->As<ColumnDateTime64>();

        ASSERT_EQ(count, slice->Size());
        ASSERT_EQ(column->GetPrecision(), slice->GetPrecision());

        for (size_t i = offset; i < offset + count; ++i) {
            ASSERT_EQ(data[i], slice->At(i - offset));
        }
    }
}

TEST(ColumnsCase, DateTime64_Slice_OUTOFBAND) {
    // Slice() shouldn't throw exceptions on invalid parameters, just clamp values to the nearest bounds.

    auto column = std::make_shared<ColumnDateTime64>(6ul);

    // Non-Empty slice on empty column
    EXPECT_EQ(0u, column->Slice(0, 10)->Size());

    const auto data = MakeDateTime64s();
    for (const auto & v : data) {
        column->Append(v);
    }

    EXPECT_EQ(column->Slice(0, data.size() + 1)->Size(), data.size());
    EXPECT_EQ(column->Slice(data.size() + 1, 1)->Size(), 0u);
    EXPECT_EQ(column->Slice(data.size() / 2, data.size() / 2 + 2)->Size(), data.size() - data.size() / 2);
}

TEST(ColumnsCase, DateTime64_Swap_EXCEPTION) {
    auto column1 = std::make_shared<ColumnDateTime64>(6ul);
    auto column2 = std::make_shared<ColumnDateTime64>(0ul);

    EXPECT_ANY_THROW(column1->Swap(*column2));
}

TEST(ColumnsCase, Date2038) {
    auto col1 = std::make_shared<ColumnDate>();
    std::time_t largeDate(25882ul * 86400ul);
    col1->Append(largeDate);

    ASSERT_EQ(col1->Size(), 1u);
    ASSERT_EQ(static_cast<std::uint64_t>(col1->At(0)), 25882ul * 86400ul);
}

TEST(ColumnsCase, EnumTest) {
    std::vector<Type::EnumItem> enum_items = {{"Hi", 1}, {"Hello", 2}};

    auto col = std::make_shared<ColumnEnum8>(Type::CreateEnum8(enum_items));
    ASSERT_TRUE(col->Type()->IsEqual(Type::CreateEnum8(enum_items)));

    col->Append(1);
    ASSERT_EQ(col->Size(), 1u);
    ASSERT_EQ(col->At(0), 1);
    ASSERT_EQ(col->NameAt(0), "Hi");

    col->Append("Hello");
    ASSERT_EQ(col->Size(), 2u);
    ASSERT_EQ(col->At(1), 2);
    ASSERT_EQ(col->NameAt(1), "Hello");

    auto col16 = std::make_shared<ColumnEnum16>(Type::CreateEnum16(enum_items));
    ASSERT_TRUE(col16->Type()->IsEqual(Type::CreateEnum16(enum_items)));
}

TEST(ColumnsCase, NullableSlice) {
    auto data = std::make_shared<ColumnUInt32>(MakeNumbers());
    auto nulls = std::make_shared<ColumnUInt8>(MakeBools());
    auto col = std::make_shared<ColumnNullable>(data, nulls);
    auto sub = col->Slice(3, 4)->As<ColumnNullable>();
    auto subData = sub->Nested()->As<ColumnUInt32>();

    ASSERT_EQ(sub->Size(), 4u);
    ASSERT_FALSE(sub->IsNull(0));
    ASSERT_EQ(subData->At(0),  7u);
    ASSERT_TRUE(sub->IsNull(1));
    ASSERT_FALSE(sub->IsNull(3));
    ASSERT_EQ(subData->At(3), 17u);
}

TEST(ColumnsCase, UUIDInit) {
    auto col = std::make_shared<ColumnUUID>(std::make_shared<ColumnUInt64>(MakeUUIDs()));

    ASSERT_EQ(col->Size(), 3u);
    ASSERT_EQ(col->At(0), UInt128(0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu));
    ASSERT_EQ(col->At(2), UInt128(0x3507213c178649f9llu, 0x9faf035d662f60aellu));
}

TEST(ColumnsCase, UUIDSlice) {
    auto col = std::make_shared<ColumnUUID>(std::make_shared<ColumnUInt64>(MakeUUIDs()));
    auto sub = col->Slice(1, 2)->As<ColumnUUID>();

    ASSERT_EQ(sub->Size(), 2u);
    ASSERT_EQ(sub->At(0), UInt128(0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu));
    ASSERT_EQ(sub->At(1), UInt128(0x3507213c178649f9llu, 0x9faf035d662f60aellu));
}

TEST(ColumnsCase, ColumnLowCardinalityString_Append_and_Read) {
    const size_t items_count = 11;
    ColumnLowCardinalityT<ColumnString> col;
    for (const auto & item : GenerateVector(items_count, &FooBarSeq)) {
        col.Append(item);
    }

    ASSERT_EQ(col.Size(), items_count);
    ASSERT_EQ(col.GetDictionarySize(), 8u + 1); // 8 unique items from sequence + 1 null-item

    for (size_t i = 0; i < items_count; ++i) {
        ASSERT_EQ(col.At(i), FooBarSeq(i)) << " at pos: " << i;
        ASSERT_EQ(col[i], FooBarSeq(i)) << " at pos: " << i;
    }
}

TEST(ColumnsCase, ColumnLowCardinalityString_Clear_and_Append) {
    const size_t items_count = 11;
    ColumnLowCardinalityT<ColumnString> col;
    for (const auto & item : GenerateVector(items_count, &FooBarSeq))
    {
        col.Append(item);
    }

    col.Clear();
    ASSERT_EQ(col.Size(), 0u);
    ASSERT_EQ(col.GetDictionarySize(), 1u); // null-item

    for (const auto & item : GenerateVector(items_count, &FooBarSeq))
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
    CodedInputStream stream(&buffer);

    EXPECT_TRUE(col.Load(&stream, items_count));

    for (size_t i = 0; i < items_count; ++i) {
        EXPECT_EQ(col.At(i), FooBarSeq(i)) << " at pos: " << i;
    }
}

// This is temporary diabled since we are not 100% compatitable with ClickHouse
// on how we serailize LC columns, but we check interoperability in other tests (see client_ut.cpp)
TEST(ColumnsCase, DISABLED_ColumnLowCardinalityString_Save) {
    const size_t items_count = 10;
    ColumnLowCardinalityT<ColumnString> col;
    for (const auto & item : GenerateVector(items_count, &FooBarSeq)) {
        col.Append(item);
    }

    ArrayOutput output(0, 0);
    CodedOutputStream output_stream(&output);

    const size_t expected_output_size = LOWCARDINALITY_STRING_FOOBAR_10_ITEMS_BINARY.size();
    // Enough space to account for possible overflow from both right and left sides.
    char buffer[expected_output_size * 10] = {'\0'};
    const char margin_content[sizeof(buffer)] = {'\0'};

    const size_t left_margin_size = 10;
    const size_t right_margin_size = sizeof(buffer) - left_margin_size - expected_output_size;

    // Since overflow from left side is less likely to happen, leave only tiny margin there.
    auto write_pos = buffer + left_margin_size;
    const auto left_margin = buffer;
    const auto right_margin = write_pos + expected_output_size;

    output.Reset(write_pos, expected_output_size);

    EXPECT_NO_THROW(col.Save(&output_stream));

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

    const auto items = GenerateVector(10, &FooBarSeq);
    for (const auto & item : items) {
        col.Append(item);
    }

    char buffer[256] = {'\0'}; // about 3 times more space than needed for this set of values.
    {
        ArrayOutput output(buffer, sizeof(buffer));
        CodedOutputStream output_stream(&output);
        EXPECT_NO_THROW(col.Save(&output_stream));
    }

    col.Clear();

    {
        // Load the data back
        ArrayInput input(buffer, sizeof(buffer));
        CodedInputStream input_stream(&input);
        EXPECT_TRUE(col.Load(&input_stream, items.size()));
    }

    for (size_t i = 0; i < items.size(); ++i) {
        EXPECT_EQ(col.At(i), items[i]) << " at pos: " << i;
    }
}

TEST(ColumnsCase, ColumnLowCardinalityString_WithEmptyString_1) {
    // Verify that when empty string is added to a LC column it can be retrieved back as empty string.
    ColumnLowCardinalityT<ColumnString> col;
    const auto values = GenerateVector(10, AlternateGenerators<std::string>(SameValueSeq<std::string>(""), FooBarSeq));
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
    const auto values = GenerateVector(10, AlternateGenerators<std::string>(FooBarSeq, SameValueSeq<std::string>("")));
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
    const auto values = ConcatSequences(GenerateVector(100, SameValueSeq<std::string>("")), GenerateVector(5, FooBarSeq));
    for (const auto & item : values) {
        col.Append(item);
    }

    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(values[i], col.At(i)) << " at pos: " << i;
    }
}

TEST(ColumnsCase, CreateSimpleAggregateFunction) {
    auto col = CreateColumnByType("SimpleAggregateFunction(funt, Int32)");

    ASSERT_EQ("Int32", col->Type()->GetName());
    ASSERT_EQ(Type::Int32, col->Type()->GetCode());
    ASSERT_NE(nullptr, col->As<ColumnInt32>());
}


TEST(CreateColumnByType, UnmatchedBrackets) {
    // When type string has unmatched brackets, CreateColumnByType must return nullptr.
    ASSERT_EQ(nullptr, CreateColumnByType("FixedString(10"));
    ASSERT_EQ(nullptr, CreateColumnByType("Nullable(FixedString(10000"));
    ASSERT_EQ(nullptr, CreateColumnByType("Nullable(FixedString(10000)"));
    ASSERT_EQ(nullptr, CreateColumnByType("LowCardinality(Nullable(FixedString(10000"));
    ASSERT_EQ(nullptr, CreateColumnByType("LowCardinality(Nullable(FixedString(10000)"));
    ASSERT_EQ(nullptr, CreateColumnByType("LowCardinality(Nullable(FixedString(10000))"));
    ASSERT_EQ(nullptr, CreateColumnByType("Array(LowCardinality(Nullable(FixedString(10000"));
    ASSERT_EQ(nullptr, CreateColumnByType("Array(LowCardinality(Nullable(FixedString(10000)"));
    ASSERT_EQ(nullptr, CreateColumnByType("Array(LowCardinality(Nullable(FixedString(10000))"));
    ASSERT_EQ(nullptr, CreateColumnByType("Array(LowCardinality(Nullable(FixedString(10000)))"));
}
