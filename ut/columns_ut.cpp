#include <clickhouse/columns/array.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/socket.h> // for ipv4-ipv6 platform-specific stuff

#include <gtest/gtest.h>
#include "utils.h"
#include "value_generators.h"

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
    const auto column_data = MakeFixedStrings(3);
    auto col = std::make_shared<ColumnFixedString>(3, column_data);

    ASSERT_EQ(col->Size(), column_data.size());

    size_t i = 0;
    for (const auto& s : column_data) {
        EXPECT_EQ(s, col->At(i));
        ++i;
    }
}

TEST(ColumnsCase, FixedString_Append_SmallStrings) {
    // Ensure that strings smaller than FixedString's size
    // are padded with zeroes on insertion.

    const size_t string_size = 7;
    const auto column_data = MakeFixedStrings(3);

    auto col = std::make_shared<ColumnFixedString>(string_size);
    size_t i = 0;
    for (const auto& s : column_data) {
        col->Append(s);

        EXPECT_EQ(string_size, col->At(i).size());

        std::string expected = column_data[i];
        expected.resize(string_size, char(0));
        EXPECT_EQ(expected, col->At(i));

        ++i;
    }

    ASSERT_EQ(col->Size(), i);
}

TEST(ColumnsCase, FixedString_Append_LargeString) {
    // Ensure that inserting strings larger than FixedString size thorws exception.

    const auto col = std::make_shared<ColumnFixedString>(1);
    EXPECT_ANY_THROW(col->Append("2c"));
    EXPECT_ANY_THROW(col->Append("this is a long string"));
}

TEST(ColumnsCase, StringInit) {
    auto values = MakeStrings();
    auto col = std::make_shared<ColumnString>(values);

    ASSERT_EQ(col->Size(), values.size());
    ASSERT_EQ(col->At(1), "ab");
    ASSERT_EQ(col->At(3), "abcd");
}

TEST(ColumnsCase, StringAppend) {
    auto col = std::make_shared<ColumnString>();
    const char* expected = "ufiudhf3493fyiudferyer3yrifhdflkdjfeuroe";
    std::string data(expected);
    col->Append(data);
    col->Append(std::move(data));
    col->Append("11");

    ASSERT_EQ(col->Size(), 3u);
    ASSERT_EQ(col->At(0), expected);
    ASSERT_EQ(col->At(1), expected);
    ASSERT_EQ(col->At(2), "11");
}

TEST(ColumnsCase, TupleAppend){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }));
    auto tuple2 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }));
    (*tuple1)[0]->As<ColumnUInt64>()->Append(2u);
    (*tuple1)[1]->As<ColumnString>()->Append("2");
    tuple2->Append(tuple1);

    ASSERT_EQ((*tuple2)[0]->As<ColumnUInt64>()->At(0), 2u);
    ASSERT_EQ((*tuple2)[1]->As<ColumnString>()->At(0), "2");
}

TEST(ColumnsCase, TupleSlice){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }));

    (*tuple1)[0]->As<ColumnUInt64>()->Append(2u);
    (*tuple1)[1]->As<ColumnString>()->Append("2");
    (*tuple1)[0]->As<ColumnUInt64>()->Append(3u);
    (*tuple1)[1]->As<ColumnString>()->Append("3");
    auto tuple2 = tuple1->Slice(1,1)->As<ColumnTuple>();

    ASSERT_EQ((*tuple2)[0]->As<ColumnUInt64>()->At(0), 3u);
    ASSERT_EQ((*tuple2)[1]->As<ColumnString>()->At(0), "3");
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


TEST(ColumnsCase, Date_UInt16_interface) {
    auto col1 = std::make_shared<ColumnDate>();

    col1->AppendRaw(1u);
    col1->AppendRaw(1234u);

    ASSERT_EQ(col1->Size(), 2u);
    ASSERT_EQ(col1->RawAt(0), 1u);
    ASSERT_EQ(col1->RawAt(1), 1234u);
}

TEST(ColumnsCase, Date_UInt16_construct_from_rvalue_data) {
    auto const expected = MakeNumbers<uint16_t>();

    auto data = expected;
    auto col1 = std::make_shared<ColumnDate>(std::move(data));

    ASSERT_EQ(col1->Size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(col1->RawAt(i), expected[i]);
    }
}

TEST(ColumnsCase, Date32_Int32_interface) {
    auto col1 = std::make_shared<ColumnDate32>();

    col1->AppendRaw(1);
    col1->AppendRaw(1234);
    col1->AppendRaw(-1234);

    ASSERT_EQ(col1->Size(), 3u);
    ASSERT_EQ(col1->RawAt(0), 1);
    ASSERT_EQ(col1->RawAt(1), 1234);
    ASSERT_EQ(col1->RawAt(2), -1234);
}

TEST(ColumnsCase, Date32_construct_from_rvalue_data) {
    auto const expected = MakeNumbers<int32_t>();

    auto data = expected;
    auto col1 = std::make_shared<ColumnDate32>(std::move(data));

    ASSERT_EQ(col1->Size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(col1->RawAt(i), expected[i]);
    }
}

TEST(ColumnsCase, DateTime_construct_from_rvalue_data) {
    auto const expected = MakeNumbers<uint32_t>();

    auto data = expected;
    auto col1 = std::make_shared<ColumnDateTime>(std::move(data));

    EXPECT_TRUE(CompareRecursive(*col1, expected));
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

    const auto data = MakeDateTime64s(6ul);
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

    const auto data = MakeDateTime64s(6ul);
    for (const auto & v : data) {
        column->Append(v);
    }

    ASSERT_NO_THROW(column->Clear());
    ASSERT_EQ(0u, column->Size());
}

TEST(ColumnsCase, DateTime64_Swap) {
    auto column = std::make_shared<ColumnDateTime64>(6ul);

    const auto data = MakeDateTime64s(6ul);
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
        auto slice = column->CloneEmpty()->As<ColumnDateTime64>();
        ASSERT_EQ(0u, slice->Size());
        ASSERT_EQ(column->GetPrecision(), slice->GetPrecision());
    }

    const auto data = MakeDateTime64s(6ul);
    const size_t size = data.size();
    ASSERT_GT(size, 4u); // so the partial slice below has half of the elements of the column

    for (const auto & v : data) {
        column->Append(v);
    }

    {
        // Empty slice on non-empty column
        auto slice = column->CloneEmpty()->As<ColumnDateTime64>();
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

    const auto data = MakeDateTime64s(6ul);
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
    const std::time_t largeDate(25882ull * 86400ull);
    col1->Append(largeDate);

    ASSERT_EQ(col1->Size(), 1u);
    ASSERT_EQ(largeDate, col1->At(0));
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

    ASSERT_TRUE(CreateColumnByType("Enum8('Hi' = 1, 'Hello' = 2)")->Type()->IsEqual(Type::CreateEnum8(enum_items)));
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

// internal representation of UUID data in ColumnUUID
std::vector<uint64_t> MakeUUID_data() {
    return {
        0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu,
        0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu,
        0x3507213c178649f9llu, 0x9faf035d662f60aellu
    };
}

TEST(ColumnsCase, UUIDInit) {
    auto col = std::make_shared<ColumnUUID>(std::make_shared<ColumnUInt64>(MakeUUID_data()));

    ASSERT_EQ(col->Size(), 3u);
    ASSERT_EQ(col->At(0), UUID(0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu));
    ASSERT_EQ(col->At(2), UUID(0x3507213c178649f9llu, 0x9faf035d662f60aellu));
}

TEST(ColumnsCase, UUIDSlice) {
    auto col = std::make_shared<ColumnUUID>(std::make_shared<ColumnUInt64>(MakeUUID_data()));
    auto sub = col->Slice(1, 2)->As<ColumnUUID>();

    ASSERT_EQ(sub->Size(), 2u);
    ASSERT_EQ(sub->At(0), UUID(0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu));
    ASSERT_EQ(sub->At(1), UUID(0x3507213c178649f9llu, 0x9faf035d662f60aellu));
}

TEST(ColumnsCase, Int128) {
    auto col = std::make_shared<ColumnInt128>(std::vector<Int128>{
            absl::MakeInt128(0xffffffffffffffffll, 0xffffffffffffffffll), // -1
            absl::MakeInt128(0, 0xffffffffffffffffll),  // 2^64
            absl::MakeInt128(0xffffffffffffffffll, 0),
            absl::MakeInt128(0x8000000000000000ll, 0),
            Int128(0)
    });

    EXPECT_EQ(-1, col->At(0));

    EXPECT_EQ(absl::MakeInt128(0, 0xffffffffffffffffll), col->At(1));
    EXPECT_EQ(0ll,                   absl::Int128High64(col->At(1)));
    EXPECT_EQ(0xffffffffffffffffull, absl::Int128Low64(col->At(1)));

    EXPECT_EQ(absl::MakeInt128(0xffffffffffffffffll, 0), col->At(2));
    EXPECT_EQ(static_cast<int64_t>(0xffffffffffffffffll),  absl::Int128High64(col->At(2)));
    EXPECT_EQ(0ull,                  absl::Int128Low64(col->At(2)));

    EXPECT_EQ(0, col->At(4));
}

TEST(ColumnsCase, ColumnIPv4)
{
    // TODO: split into proper method-level unit-tests
    auto col = ColumnIPv4();

    col.Append("255.255.255.255");
    col.Append("127.0.0.1");
    col.Append(3585395774);
    col.Append(0);
    const in_addr ip = MakeIPv4(0x12345678);
    col.Append(ip);

    ASSERT_EQ(5u, col.Size());
    EXPECT_EQ(MakeIPv4(0xffffffff), col.At(0));
    EXPECT_EQ(MakeIPv4(0x0100007f), col.At(1));
    EXPECT_EQ(MakeIPv4(3585395774), col.At(2));
    EXPECT_EQ(MakeIPv4(0),          col.At(3));
    EXPECT_EQ(ip,                  col.At(4));

    EXPECT_EQ("255.255.255.255", col.AsString(0));
    EXPECT_EQ("127.0.0.1",       col.AsString(1));
    EXPECT_EQ("62.204.180.213",  col.AsString(2));
    EXPECT_EQ("0.0.0.0",         col.AsString(3));
    EXPECT_EQ("120.86.52.18",    col.AsString(4));

    col.Clear();
    EXPECT_EQ(0u, col.Size());
}

TEST(ColumnsCase, ColumnIPv4_construct_from_data)
{
    const auto vals = {
        MakeIPv4(0x12345678),
        MakeIPv4(0x0),
        MakeIPv4(0x0100007f)
    };

    {
        // Column is usable after being initialized with empty data column
        auto col = ColumnIPv4(std::make_shared<ColumnUInt32>());
        EXPECT_EQ(0u, col.Size());

        // Make sure that `Append` and `At`/`[]` work properly
        size_t i = 0;
        for (const auto & v : vals) {
            col.Append(v);
            EXPECT_EQ(v, col[col.Size() - 1]) << "At pos " << i;
            EXPECT_EQ(v, col.At(col.Size() - 1)) << "At pos " << i;
            ++i;
        }

        EXPECT_EQ(vals.size(), col.Size());
    }

    {
        // Column reports values from data column exactly, and also can be modified afterwards.
        const auto values = std::vector<uint32_t>{std::numeric_limits<uint32_t>::min(), 123, 456, 789101112, std::numeric_limits<uint32_t>::max()};
        auto col = ColumnIPv4(std::make_shared<ColumnUInt32>(values));

        EXPECT_EQ(values.size(), col.Size());
        for (size_t i = 0; i < values.size(); ++i) {
            EXPECT_EQ(ntohl(values[i]), col[i]) << " At pos: " << i;
        }

        // Make sure that `Append` and `At`/`[]` work properly
        size_t i = 0;
        for (const auto & v : vals) {
            col.Append(v);
            EXPECT_EQ(v, col[col.Size() - 1]) << "At pos " << i;
            EXPECT_EQ(v, col.At(col.Size() - 1)) << "At pos " << i;
            ++i;
        }

        EXPECT_EQ(values.size() + vals.size(), col.Size());
    }

    EXPECT_ANY_THROW(ColumnIPv4(nullptr));
    EXPECT_ANY_THROW(ColumnIPv4(ColumnRef(std::make_shared<ColumnInt8>())));
    EXPECT_ANY_THROW(ColumnIPv4(ColumnRef(std::make_shared<ColumnInt32>())));

    EXPECT_ANY_THROW(ColumnIPv4(ColumnRef(std::make_shared<ColumnUInt8>())));

    EXPECT_ANY_THROW(ColumnIPv4(ColumnRef(std::make_shared<ColumnInt128>())));
    EXPECT_ANY_THROW(ColumnIPv4(ColumnRef(std::make_shared<ColumnString>())));
}

TEST(ColumnsCase, ColumnIPv4_construct_from_rvalue_data) {
    std::vector<uint32_t> data = {
        0x12345678,
        0x0,
        0x0100007f,
    };

    const auto expected = {
        MakeIPv4(data[0]),
        MakeIPv4(data[1]),
        MakeIPv4(data[2]),
    };

    auto col = ColumnIPv4(std::move(data));
    EXPECT_TRUE(CompareRecursive(col, expected));
}

TEST(ColumnsCase, ColumnIPv6)
{
    // TODO: split into proper method-level unit-tests
    auto col = ColumnIPv6();
    col.Append("0:0:0:0:0:0:0:1");
    col.Append("::");
    col.Append("::FFFF:204.152.189.116");

    const auto ipv6 = MakeIPv6(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    col.Append(ipv6);

    ASSERT_EQ(4u, col.Size());
    EXPECT_EQ(MakeIPv6(0, 0, 0, 0, 0, 1),               col.At(0));
    EXPECT_EQ(MakeIPv6(0, 0, 0, 0, 0, 0),               col.At(1));
    EXPECT_EQ(MakeIPv6(0xff, 0xff, 204, 152, 189, 116), col.At(2));

    EXPECT_EQ(ipv6, col.At(3));

    EXPECT_EQ("::1",                    col.AsString(0));
    EXPECT_EQ("::",                     col.AsString(1));
    EXPECT_EQ("::ffff:204.152.189.116", col.AsString(2));
    EXPECT_EQ("1:203:405:607:809:a0b:c0d:e0f", col.AsString(3));

    col.Clear();
    EXPECT_EQ(0u, col.Size());
}

TEST(ColumnsCase, ColumnIPv6_construct_from_data)
{
    const auto vals = {
        MakeIPv6(0xff, 0xff, 204, 152, 189, 116),
        MakeIPv6(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
    };

    {
        // Column is usable after being initialized with empty data column
        auto col = ColumnIPv6(std::make_shared<ColumnFixedString>(16));
        EXPECT_EQ(0u, col.Size());

        // Make sure that `Append` and `At`/`[]` work properly
        size_t i = 0;
        for (const auto & v : vals) {
            col.Append(v);
            EXPECT_EQ(v, col[col.Size() - 1]) << "At pos " << i;
            EXPECT_EQ(v, col.At(col.Size() - 1)) << "At pos " << i;
            ++i;
        }

        EXPECT_EQ(vals.size(), col.Size());
    }

    {
        // Column reports values from data column exactly, and also can be modified afterwards.
        using namespace std::literals;
        const auto values = std::vector<std::string_view>{
                "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"sv,
                "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F"sv,
                "\xF0\xF1\xF2\xF3\xF4\xF5\xF6\xF7\xF8\xF9\xFA\xFB\xFC\xFD\xFE\xFF"sv};
        auto col = ColumnIPv6(std::make_shared<ColumnFixedString>(16, values));

        EXPECT_EQ(values.size(), col.Size());
        for (size_t i = 0; i < values.size(); ++i) {
            EXPECT_EQ(values[i], col[i]) << " At pos: " << i;
        }

        // Make sure that `Append` and `At`/`[]` work properly
        size_t i = 0;
        for (const auto & v : vals) {
            col.Append(v);
            EXPECT_EQ(v, col[col.Size() - 1]) << "At pos " << i;
            EXPECT_EQ(v, col.At(col.Size() - 1)) << "At pos " << i;
            ++i;
        }

        EXPECT_EQ(values.size() + vals.size(), col.Size());
    }

    // Make sure that column can't be constructed with wrong data columns (wrong size/wrong type or null)
    EXPECT_ANY_THROW(ColumnIPv4(nullptr));
    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnFixedString>(15))));
    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnFixedString>(17))));

    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnInt8>())));
    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnInt32>())));

    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnUInt8>())));

    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnInt128>())));
    EXPECT_ANY_THROW(ColumnIPv6(ColumnRef(std::make_shared<ColumnString>())));
}

TEST(ColumnsCase, ColumnDecimal128_from_string) {
    auto col = std::make_shared<ColumnDecimal>(38, 0);

    const auto values = {
        Int128(0),
        Int128(-1),
        Int128(1),
        std::numeric_limits<Int128>::min() + 1,
        std::numeric_limits<Int128>::max(),
    };

    for (size_t i = 0; i < values.size(); ++i) {
        const auto value = values.begin()[i];
        SCOPED_TRACE(::testing::Message() << "# index: " << i << " Int128 value: " << value);

        {
            std::stringstream sstr;
            sstr << value;
            const auto string_value = sstr.str();

            EXPECT_NO_THROW(col->Append(string_value));
        }

        ASSERT_EQ(i + 1, col->Size());
        EXPECT_EQ(value, col->At(i));
    }
}

TEST(ColumnsCase, ColumnDecimal128_from_string_overflow) {
    auto col = std::make_shared<ColumnDecimal>(38, 0);

    // 2^128 overflows
    EXPECT_ANY_THROW(col->Append("340282366920938463463374607431768211456"));
    // special case for number bigger than 2^128, ending in zeroes.
    EXPECT_ANY_THROW(col->Append("400000000000000000000000000000000000000"));

#ifndef ABSL_HAVE_INTRINSIC_INT128
    // unfortunately std::numeric_limits<Int128>::min() overflows when there is no __int128 intrinsic type.
    EXPECT_ANY_THROW(col->Append("-170141183460469231731687303715884105728"));
#endif
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


TEST(ColumnsCase, ColumnTupleT) {
    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString, ColumnFixedString>;

    TestTuple col(
        std::make_tuple(
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>(),
            std::make_shared<ColumnFixedString>(3)
        )
    );
    const auto val = std::make_tuple(1, "a", "bcd");
    col.Append(val);
    static_assert(std::is_same_v<uint64_t, std::tuple_element<0,decltype(col.At(0))>::type>);
    static_assert(std::is_same_v<std::string_view, std::tuple_element<1,decltype(col.At(0))>::type>);
    static_assert(std::is_same_v<std::string_view, std::tuple_element<2,decltype(col.At(0))>::type>);
    EXPECT_EQ(val, col.At(0));
}

TEST(ColumnsCase, ColumnTupleT_Wrap) {
    ColumnTuple col ({
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>(),
            std::make_shared<ColumnFixedString>(3)
        }
    );

    const auto val = std::make_tuple(1, "a", "bcd");

    col[0]->AsStrict<ColumnUInt64>()->Append(std::get<0>(val));
    col[1]->AsStrict<ColumnString>()->Append(std::get<1>(val));
    col[2]->AsStrict<ColumnFixedString>()->Append(std::get<2>(val));

    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString, ColumnFixedString>;
    auto wrapped_col = TestTuple::Wrap(std::move(col));

    EXPECT_EQ(wrapped_col->Size(), 1u);
    EXPECT_EQ(val, wrapped_col->At(0));
}

TEST(ColumnsCase, ColumnTupleT_Empty) {
    using TestTuple = ColumnTupleT<>;

    TestTuple col(std::make_tuple());
    const auto val = std::make_tuple();
    col.Append(val);
    EXPECT_EQ(col.Size(), 0u);
}

TEST(ColumnsCase, ColumnMapT) {
    ColumnMapT<ColumnUInt64, ColumnString> col(
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>());

    std::map<uint64_t, std::string> val;
    val[1] = "123";
    val[2] = "abc";
    col.Append(val);

    auto map_view = col.At(0);

    EXPECT_THROW(map_view.At(0), ValidationError);
    EXPECT_EQ(val[1], map_view.At(1));
    EXPECT_EQ(val[2], map_view.At(2));

    std::map<uint64_t, std::string_view> map{map_view.begin(), map_view.end()};

    EXPECT_EQ(val[1], map.at(1));
    EXPECT_EQ(val[2], map.at(2));
}

TEST(ColumnsCase, ColumnMapT_Wrap) {
    auto tupls = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>()});

    auto data = std::make_shared<ColumnArray>(tupls);

    auto val = tupls->CloneEmpty()->As<ColumnTuple>();

    (*val)[0]->AsStrict<ColumnUInt64>()->Append(1);
    (*val)[1]->AsStrict<ColumnString>()->Append("123");

    (*val)[0]->AsStrict<ColumnUInt64>()->Append(2);
    (*val)[1]->AsStrict<ColumnString>()->Append("abc");

    data->AppendAsColumn(val);

    ColumnMap col{data};

    using TestMap = ColumnMapT<ColumnUInt64, ColumnString>;
    auto wrapped_col = TestMap::Wrap(std::move(col));

    auto map_view = wrapped_col->At(0);

    EXPECT_THROW(map_view.At(0), ValidationError);
    EXPECT_EQ("123", map_view.At(1));
    EXPECT_EQ("abc", map_view.At(2));
}
