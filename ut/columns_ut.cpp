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
#include <clickhouse/types/bignum.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/socket.h> // for ipv4-ipv6 platform-specific stuff

#include <gtest/gtest.h>
#include "utils.h"
#include "value_generators.h"

#include <memory>
#include <string_view>
#include <vector>

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

class ColumnDecimalFromString : public ::testing::TestWithParam<std::pair<size_t, size_t>> {};

TEST_P(ColumnDecimalFromString, DecimalFromGoodString)
{
    const auto [precision, scale] = GetParam();
    size_t whole = precision - scale;
    std::vector<std::pair<std::string, std::string>> values = {
        {"0", "0"},
        {"-0", "0"},
        {"0.", "0"},
        {"-0.", "0"},
        {"1", "1"},
        {"-1", "-1"},
        {"1.0", "1" + std::string(scale, '0')},
        {"-1.0", "-1" + std::string(scale, '0')},
        {"1.", "1" + std::string(scale, '0')},
        {"-1.", "-1" + std::string(scale, '0')},
        {std::string(whole, '9') + ".", std::string(whole, '9') + std::string(scale, '0')},
        {"-" + std::string(whole, '9') + ".", "-" + std::string(whole, '9') + std::string(scale, '0')},
        {std::string(precision, '9'), std::string(precision, '9')},
        {"-" + std::string(precision, '9'), "-" + std::string(precision, '9')},
    };

    for (auto & [value, expect] : values) {
        auto col = std::make_shared<ColumnDecimal>(precision, scale);
        EXPECT_NO_THROW(col->Append(value)) << "exception for value \"" << value << "\"";
        ASSERT_EQ(expect, Bignum::Int128ToString(col->At(0)));
    }
}

TEST_P(ColumnDecimalFromString, DecimalFromBadString)
{
    const auto [precision, scale] = GetParam();
    size_t whole = precision - scale;

    std::vector<std::string> values = {
        "",
        ".",
        " ",
        "-.",
        "--0",
        "1.2222x",
        "0-",
        ".0.",
        "+5",
        " 5",
        "50 000",
        "1e3",
        "0x1",
        "1,1234.5",

        std::string(whole + 1, '9') + "." + std::string(scale, '0'),
    };
    for (auto value : values) {
        auto col = std::make_shared<ColumnDecimal>(precision, scale);
        EXPECT_THROW(
            col->Append(value),
            ValidationError
        ) << "got \"" << Bignum::Int128ToString(col->At(0)) << "\" for value \"" << value << "\"";
    }
}

INSTANTIATE_TEST_SUITE_P(
    ColumnCase,
    ColumnDecimalFromString,
    ::testing::Values(
        std::make_pair<size_t, size_t>(9, 0),
        std::make_pair<size_t, size_t>(9, 4),
        std::make_pair<size_t, size_t>(18, 6),
        std::make_pair<size_t, size_t>(15, 2),
        std::make_pair<size_t, size_t>(38, 0),
        std::make_pair<size_t, size_t>(38, 4)
    ),
    [](const ::testing::TestParamInfo<std::pair<size_t, size_t>>& info) {
        return "p" + std::to_string(info.param.first)
             + "_s" + std::to_string(info.param.second);
    });

TEST(ColumnsCase, DecimalStringValueMapping) {
    struct TestSample {
        size_t precision;
        size_t scale;
        std::string str;
        Int128 expect;
    };
    
    std::vector<TestSample> samples = {
        {18, 0, "0.0", Int128(0)},
        {18, 0, "0.123", Int128(0)},
        {18, 0, "123", Int128(123)},
        {18, 3, "0.123", Int128(123)},
        {18, 3, "1234", Int128(1234)},
        {18, 3, "1.234", Int128(1234)},
        {18, 3, "0.1234", Int128(123)},
        {18, 3, "12", Int128(12)},
        {18, 3, "12.0", Int128(12000)},
        {18, 3, "12.", Int128(12000)},
    };

    for (auto & [precision, scale, str, expect] : samples) {
        auto col = std::make_shared<ColumnDecimal>(precision, scale);
        EXPECT_NO_THROW(col->Append(str)) << "exception for value \"" << str << "\"";
        auto value = col->At(0);
        EXPECT_EQ(value, expect);
    }

}

TEST(ColumnsCase, DecimalStringAt) {
    struct TestSample {
        size_t precision;
        size_t scale;
        std::string str;
        std::string expect;
    };

    std::vector<TestSample> samples = {
        {18, 0, "0", "0"},
        {18, 0, "123", "123"},
        {18, 0, "-123", "-123"},
        {18, 3, "0", "0.000"},
        {18, 3, "1", "0.001"},
        {18, 3, "12", "0.012"},
        {18, 3, "123", "0.123"},
        {18, 3, "-123", "-0.123"},
        {18, 3, "0.123", "0.123"},
        {18, 3, "1234", "1.234"},
        {18, 3, "-1234", "-1.234"},
        {18, 3, "-1.234", "-1.234"},
        {18, 3, "123.0", "123.000"},
        {18, 3, "123.", "123.000"},
        {18, 3, "123000", "123.000"},
        // Large values that do not fit in 64 bits.
        {38, 4, "12345678901234567890.1234", "12345678901234567890.1234"},
        {38, 4, "-12345678901234567890.1234", "-12345678901234567890.1234"},
        {38, 0, "99999999999999999999999999999999999999", "99999999999999999999999999999999999999"},
    };

    for (auto & [precision, scale, str, expect] : samples) {
        auto col = std::make_shared<ColumnDecimal>(precision, scale);
        EXPECT_NO_THROW(col->Append(str)) << "exception for value \"" << str << "\"";
        auto value = col->StringAt(0);
        EXPECT_EQ(value, expect);
    }

}

TEST(ColumnsCase, DecimalSwap) {
    auto column1 = std::make_shared<ColumnDecimal>(18, 2);
    auto column2 = std::make_shared<ColumnDecimal>(18, 2);
    column1->Append("1.23");
    column2->Append("4.56");

    column1->Swap(*column2);

    EXPECT_EQ(column1->StringAt(0), "4.56");
    EXPECT_EQ(column2->StringAt(0), "1.23");
}

TEST(ColumnsCase, DecimalSwapDifferentScale) {
    auto column1 = std::make_shared<ColumnDecimal>(18, 2);
    auto column2 = std::make_shared<ColumnDecimal>(18, 3);
    column1->Append("1.23");
    column2->Append("4.567");

    EXPECT_THROW(column1->Swap(*column2), ValidationError);

    EXPECT_EQ(column1->GetType().GetName(), "Decimal(18,2)");
    EXPECT_EQ(column2->GetType().GetName(), "Decimal(18,3)");
    EXPECT_EQ(column1->StringAt(0), "1.23");
    EXPECT_EQ(column2->StringAt(0), "4.567");
}

TEST(ColumnsCase, DecimalSwapDifferentPrecision) {
    auto column1 = std::make_shared<ColumnDecimal>(9, 2);
    auto column2 = std::make_shared<ColumnDecimal>(18, 2);
    column1->Append("1.23");
    column2->Append("4.56");

    EXPECT_THROW(column1->Swap(*column2), ValidationError);

    EXPECT_EQ(column1->GetType().GetName(), "Decimal(9,2)");
    EXPECT_EQ(column2->GetType().GetName(), "Decimal(18,2)");
    EXPECT_EQ(column1->StringAt(0), "1.23");
    EXPECT_EQ(column2->StringAt(0), "4.56");
}

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

TEST(ColumnsCase, FixedString_Type_Size_Eq0) {
    const auto col = std::make_shared<ColumnFixedString>(0);
    ASSERT_EQ(col->FixedSize(), col->Type()->As<FixedStringType>()->GetSize());
}

TEST(ColumnsCase, FixedString_Type_Size_Eq10) {
    const auto col = std::make_shared<ColumnFixedString>(10);
    ASSERT_EQ(col->FixedSize(), col->Type()->As<FixedStringType>()->GetSize());
}

TEST(ColumnsCase, FixedStringSwap) {
    auto column1 = std::make_shared<ColumnFixedString>(2);
    auto column2 = std::make_shared<ColumnFixedString>(2);
    column1->Append("aa");
    column2->Append("bb");

    column1->Swap(*column2);

    EXPECT_EQ(column1->At(0), "bb");
    EXPECT_EQ(column2->At(0), "aa");
}

TEST(ColumnsCase, FixedStringSwapDifferentSize) {
    auto column1 = std::make_shared<ColumnFixedString>(2);
    auto column2 = std::make_shared<ColumnFixedString>(4);
    column1->Append("aa");
    column2->Append("bbbb");

    EXPECT_THROW(column1->Swap(*column2), ValidationError);

    EXPECT_EQ(column1->FixedSize(), 2u);
    EXPECT_EQ(column2->FixedSize(), 4u);
    EXPECT_EQ(column1->At(0), "aa");
    EXPECT_EQ(column2->At(0), "bbbb");
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

TEST(ColumnsCase, BoolInit)
{
    auto values = MakeBools();
    auto col = std::make_shared<ColumnBool>(values);

    ASSERT_EQ(col->Size(), values.size());
    ASSERT_EQ(col->At(0), 1);
    ASSERT_EQ(col->At(3), 0);
}

TEST(ColumnsCase, BoolAppend)
{
    auto col = std::make_shared<ColumnBool>();
    col->Append(true);
    col->Append(false);

    ASSERT_EQ(col->Size(), 2u);
    ASSERT_EQ(col->At(0), true);
    ASSERT_EQ(col->At(1), false);
}

TEST(ColumnsCase, JSONInit) {
    auto values = MakeJSONs();
    auto col = std::make_shared<ColumnJSON>(values);

    ASSERT_EQ(col->Size(), values.size());
    ASSERT_EQ(col->At(1), values[1]);
    ASSERT_EQ(col->At(2), values[2]);
    ASSERT_EQ(col->At(3), values[3]);
}

TEST(ColumnsCase, JSONAppend) {
    auto col = std::make_shared<ColumnJSON>();
    const char* expected = "\"ufiudhf3493fyiudferyer3yrifhdflkdjfeuroe\"";
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

TEST(ColumnsCase, TupleAppendWithSameFieldNames){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }), std::vector<std::string>{"a", "b"});
    auto tuple2 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }), std::vector<std::string>{"a", "b"});
    (*tuple1)[0]->As<ColumnUInt64>()->Append(2u);
    (*tuple1)[1]->As<ColumnString>()->Append("2");
    tuple2->Append(tuple1);

    ASSERT_EQ((*tuple2)[0]->As<ColumnUInt64>()->At(0), 2u);
    ASSERT_EQ((*tuple2)[1]->As<ColumnString>()->At(0), "2");
}

TEST(ColumnsCase, TupleAppendUnnamedSourceIntoNamedDestination){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }));
    auto tuple2 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }), std::vector<std::string>{"a", "b"});
    (*tuple1)[0]->As<ColumnUInt64>()->Append(2u);
    (*tuple1)[1]->As<ColumnString>()->Append("2");
    tuple2->Append(tuple1);

    ASSERT_EQ((*tuple2)[0]->As<ColumnUInt64>()->At(0), 2u);
    ASSERT_EQ((*tuple2)[1]->As<ColumnString>()->At(0), "2");
}

TEST(ColumnsCase, TupleAppendWithDifferentFieldNames){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }), std::vector<std::string>{"x", "y"});
    auto tuple2 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }), std::vector<std::string>{"a", "b"});

    (*tuple1)[0]->As<ColumnUInt64>()->Append(2u);
    (*tuple1)[1]->As<ColumnString>()->Append("2");
    tuple2->Append(tuple1);

    ASSERT_EQ((*tuple2)[0]->As<ColumnUInt64>()->At(0), 2u);
    ASSERT_EQ((*tuple2)[1]->As<ColumnString>()->At(0), "2");
}

TEST(ColumnsCase, TupleAppendNamedSourceIntoUnnamedDestination){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }), std::vector<std::string>{"a", "b"});
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

TEST(ColumnsCase, TupleAppendRejectsIncompatibleStructure){
    auto tuple1 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnUInt64>()
                            }));
    auto tuple2 = std::make_shared<ColumnTuple>(std::vector<ColumnRef>({
                                std::make_shared<ColumnUInt64>(),
                                std::make_shared<ColumnString>()
                            }));

    EXPECT_THROW(tuple2->Append(tuple1), ValidationError);
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

TEST(ColumnsCase, TupleWithQuotedFieldNames) {
    auto col = CreateColumnByType("Tuple(`a.b` Int8, `c.d` String)");
    ASSERT_NE(col, nullptr);
    const auto& names = col->AsStrict<ColumnTuple>()->Type()->As<TupleType>()->GetItemNames();
    ASSERT_EQ(names.size(), 2u);
    EXPECT_EQ(names[0], "a.b");
    EXPECT_EQ(names[1], "c.d");
}

TEST(ColumnsCase, TimeAppend) {
    auto col = std::make_shared<ColumnTime>();
    col->Append(1);
    col->Append(60);
    ASSERT_EQ(col->Size(), 2u);
    ASSERT_EQ(col->At(0), 1);
    ASSERT_EQ(col->At(1), 60);
}

TEST(ColumnsCase, Time_Int32_construct_from_rvalue_data) {
    auto const expected = MakeNumbers<int32_t>();

    auto data = expected;
    auto col = std::make_shared<ColumnTime>(std::move(data));

    ASSERT_EQ(col->Size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(col->At(i), expected[i]);
    }
}

TEST(ColumnsCase, Time64Append) {
    auto col = std::make_shared<ColumnTime64>(3);
    col->Append(1);
    col->Append(60);
    ASSERT_EQ(col->Size(), 2u);
    ASSERT_EQ(col->At(0), 1);
    ASSERT_EQ(col->At(1), 60);
}

TEST(ColumnsCase, Time64_Int64_construct_from_rvalue_data) {
    auto const expected = MakeNumbers<int64_t>();

    auto data = expected;
    auto col1 = std::make_shared<ColumnTime64>(3, std::move(data));

    ASSERT_EQ(col1->Size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(col1->At(i), expected[i]);
    }
    ASSERT_EQ(col1->GetPrecision(), 3UL);
}

TEST(ColumnsCase, Time64SwapDifferentPrecision) {
    auto col1 = std::make_shared<ColumnTime64>(3);
    auto col2 = std::make_shared<ColumnTime64>(6);
    col1->Append(1);
    col1->Append(60);
    EXPECT_THROW(col1->Swap(*col2), ValidationError) ;
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

TEST(ColumnsCase, EnumCheckValue) {
    auto column = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"Hi", 1}, {"Hello", 2}}));

    EXPECT_NO_THROW(column->Append(1, true));
    EXPECT_THROW(column->Append(3, true), ValidationError);
    EXPECT_EQ(column->Size(), 1u);

    EXPECT_NO_THROW(column->SetAt(0, 2, true));
    EXPECT_EQ(column->At(0), 2);

    EXPECT_THROW(column->SetAt(0, 3, true), ValidationError);
    EXPECT_EQ(column->At(0), 2);
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
            Bignum::MakeInt128(0xffffffffffffffffll, 0xffffffffffffffffll), // -1
            Bignum::MakeInt128(0, 0xffffffffffffffffll),  // 2^64
            Bignum::MakeInt128(0xffffffffffffffffll, 0),
            Bignum::MakeInt128(0x8000000000000000ll, 0),
            Int128(0)
    });

    EXPECT_EQ(Int128(-1), col->At(0));

    EXPECT_EQ(Bignum::MakeInt128(0, 0xffffffffffffffffll), col->At(1));
    EXPECT_EQ(0ll,                   Bignum::Int128High64(col->At(1)));
    EXPECT_EQ(0xffffffffffffffffull, Bignum::Int128Low64(col->At(1)));

    EXPECT_EQ(Bignum::MakeInt128(0xffffffffffffffffll, 0), col->At(2));
    EXPECT_EQ(static_cast<int64_t>(0xffffffffffffffffll),  Bignum::Int128High64(col->At(2)));
    EXPECT_EQ(0ull,                  Bignum::Int128Low64(col->At(2)));

    EXPECT_EQ(Int128(0), col->At(4));
}

TEST(ColumnsCase, UInt128) {
    auto col = std::make_shared<ColumnUInt128>(std::vector<UInt128>{
            Bignum::MakeUInt128(0xffffffffffffffffll, 0xffffffffffffffffll), // 2^128 - 1
            Bignum::MakeUInt128(0, 0xffffffffffffffffll),  // 2^64 - 1
            Bignum::MakeUInt128(0xffffffffffffffffll, 0),  // 2^128 - 2^64
            Bignum::MakeUInt128(0x8000000000000000ll, 0),
            UInt128(0)
    });

    EXPECT_EQ(Bignum::MakeUInt128(0xffffffffffffffffll, 0xffffffffffffffffll), col->At(0));

    EXPECT_EQ(Bignum::MakeUInt128(0, 0xffffffffffffffffll), col->At(1));
    EXPECT_EQ(0ull,                  Bignum::UInt128High64(col->At(1)));
    EXPECT_EQ(0xffffffffffffffffull, Bignum::UInt128Low64(col->At(1)));

    EXPECT_EQ(Bignum::MakeUInt128(0xffffffffffffffffll, 0), col->At(2));
    EXPECT_EQ(static_cast<uint64_t>(0xffffffffffffffffull),  Bignum::UInt128High64(col->At(2)));
    EXPECT_EQ(0ull,                  Bignum::UInt128Low64(col->At(2)));

    EXPECT_EQ(UInt128(0), col->At(4));
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

TEST(ColumnsCase, ColumnDecimal128FromString) {
    auto col = std::make_shared<ColumnDecimal>(38, 0);

    const auto values = {
        Bignum::StringToInt128('-' + std::string(38, '9')),
        Int128(0),
        Int128(-1),
        Int128(1),
        Bignum::StringToInt128(std::string(38, '9')),
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

TEST(ColumnsCase, ColumnLowCardinalityT_Wrap_DoesNotStealSource) {
    // Populate via the typed column (the only ergonomic per-value insert path), then Wrap it
    // through an untyped ColumnRef handle, as with a column received from a query.
    auto source = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
    source->Append("a");
    source->Append("b");
    source->Append("a");

    ColumnRef untyped = source;
    auto wrapped = ColumnLowCardinalityT<ColumnString>::Wrap(untyped);

    // Wrapper reads the same data.
    ASSERT_EQ(wrapped->Size(), 3u);
    EXPECT_EQ(wrapped->At(0), "a");
    EXPECT_EQ(wrapped->At(1), "b");
    EXPECT_EQ(wrapped->At(2), "a");

    // Source (and the untyped handle) are left intact after Wrap (non-stealing).
    EXPECT_NE(untyped, nullptr);
    ASSERT_EQ(source->Size(), 3u);
    EXPECT_EQ(source->At(0), "a");
    EXPECT_EQ(source->At(2), "a");

    // Storage (dictionary + index + dedup map) is shared and stays coherent:
    // a new unique value appended via the source, and a repeat appended via the wrapper.
    const auto dict_before = source->GetDictionarySize();
    source->Append("c");     // new unique -> dictionary grows, visible via the wrapper
    wrapped->Append("a");    // repeat -> deduped against the shared map, no dictionary growth

    EXPECT_EQ(source->Size(), 5u);
    EXPECT_EQ(wrapped->Size(), 5u);
    EXPECT_EQ(wrapped->At(3), "c");
    EXPECT_EQ(wrapped->At(4), "a");
    EXPECT_EQ(source->At(4), "a");
    // "c" added exactly one dictionary entry; "a" added none (shared dedup map).
    EXPECT_EQ(source->GetDictionarySize(), dict_before + 1);
    EXPECT_EQ(wrapped->GetDictionarySize(), dict_before + 1);
}

TEST(ColumnsCase, ColumnLowCardinalityT_Swap_VisibleThroughAlias) {
    auto a = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
    a->Append("a");
    a->Append("b");
    a->Append("a");
    auto b = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
    b->Append("x");

    // Typed view of `a`, created BEFORE the swap; shares a's dictionary and index bundle.
    auto alias_a = ColumnLowCardinalityT<ColumnString>::Wrap(a);
    ASSERT_NE(alias_a, nullptr);
    ASSERT_EQ(alias_a->Size(), 3u);

    a->Swap(*b);

    // a now holds b's data, b holds a's data.
    ASSERT_EQ(a->Size(), 1u);
    EXPECT_EQ(a->At(0), "x");
    ASSERT_EQ(b->Size(), 3u);
    EXPECT_EQ(b->At(0), "a");
    EXPECT_EQ(b->At(1), "b");

    // The swap is visible through the pre-existing alias: dictionary swapped in place and the
    // shared index bundle swapped, so alias_a reflects b's data (size flips 3 -> 1).
    ASSERT_EQ(alias_a->Size(), 1u);
    EXPECT_EQ(alias_a->At(0), "x");
}

TEST(ColumnsCase, ColumnLowCardinalityT_Wrap_ThenLoad_VisibleThroughAlias) {
    // Serialize a source LowCardinality column.
    ColumnLowCardinalityT<ColumnString> src;
    src.Append("p");
    src.Append("q");
    src.Append("p");

    char buffer[256] = {'\0'};
    {
        ArrayOutput output(buffer, sizeof(buffer));
        EXPECT_NO_THROW(src.Save(&output));
    }

    // A different target column, wrapped BEFORE the load.
    auto target = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
    target->Append("z");
    auto alias = ColumnLowCardinalityT<ColumnString>::Wrap(target);
    ASSERT_NE(alias, nullptr);
    ASSERT_EQ(alias->Size(), 1u);

    // Load src's data into target: LoadBody swaps the dictionary in place and REPLACES the index
    // column inside the shared bundle.
    {
        ArrayInput input(buffer, sizeof(buffer));
        EXPECT_TRUE(target->Load(&input, 3));
    }

    ASSERT_EQ(target->Size(), 3u);
    // The pre-existing alias reflects the loaded data (shared dictionary + index bundle).
    ASSERT_EQ(alias->Size(), 3u);
    EXPECT_EQ(alias->At(0), "p");
    EXPECT_EQ(alias->At(1), "q");
    EXPECT_EQ(alias->At(2), "p");
}

TEST(ColumnsCase, ColumnLowCardinalityT_Wrap_AcceptsLvalue) {
    auto source = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
    source->Append("x");
    source->Append("y");

    using LC = ColumnLowCardinalityT<ColumnString>;

    // Non-const lvalue (untyped base reference), no std::move required.
    ColumnLowCardinality& base = *source;
    auto w1 = LC::Wrap(base);
    EXPECT_EQ(w1->At(0), "x");

    // Const lvalue.
    const ColumnLowCardinality& cref = *source;
    auto w2 = LC::Wrap(cref);
    EXPECT_EQ(w2->At(1), "y");

    // Lvalue ColumnRef, no std::move required and not consumed.
    ColumnRef ref = source;
    auto w3 = LC::Wrap(ref);
    EXPECT_EQ(w3->Size(), 2u);
    EXPECT_NE(ref, nullptr);
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

TEST(ColumnsCase, ColumnLowCardinalityFixedString_Type_Size_Eq) {
    const size_t fixed_size = 10;
    const auto col          = std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(fixed_size);
    
    ASSERT_EQ(fixed_size, col->GetNestedType()->As<FixedStringType>()->GetSize());
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

TEST(ColumnsCase, ColumnNullableT_Wrap_DoesNotStealSource) {
    auto nested = std::make_shared<ColumnUInt64>();
    auto nulls = std::make_shared<ColumnUInt8>();
    ColumnNullable col(nested, nulls);

    col.Append(false);
    nested->Append(1);
    col.Append(true);
    nested->Append(0);

    using TestNullable = ColumnNullableT<ColumnUInt64>;
    auto wrapped = TestNullable::Wrap(std::move(col));

    // Wrapper sees the same data.
    EXPECT_EQ(wrapped->Size(), 2u);
    EXPECT_EQ(wrapped->At(0), std::optional<uint64_t>(1));
    EXPECT_EQ(wrapped->At(1), std::optional<uint64_t>{});

    // Source column is left intact after Wrap (non-stealing).
    EXPECT_EQ(col.Size(), 2u);
    EXPECT_FALSE(col.IsNull(0));
    EXPECT_TRUE(col.IsNull(1));

    // Storage is shared: appending through the original is visible via the wrapper.
    col.Append(false);
    nested->Append(42);
    EXPECT_EQ(wrapped->Size(), 3u);
    EXPECT_EQ(wrapped->At(2), std::optional<uint64_t>(42));
}

TEST(ColumnsCase, ColumnNullableT_Wrap_AcceptsLvalue) {
    auto nested = std::make_shared<ColumnUInt64>();
    auto nulls = std::make_shared<ColumnUInt8>();
    ColumnNullable col(nested, nulls);
    col.Append(false);
    nested->Append(7);

    using TestNullable = ColumnNullableT<ColumnUInt64>;

    // Non-const lvalue concrete column, no std::move required.
    auto w1 = TestNullable::Wrap(col);
    EXPECT_EQ(w1->At(0), std::optional<uint64_t>(7));

    // Const lvalue concrete column.
    const ColumnNullable& cref = col;
    auto w2 = TestNullable::Wrap(cref);
    EXPECT_EQ(w2->At(0), std::optional<uint64_t>(7));

    // Lvalue ColumnRef, no std::move required and not consumed.
    ColumnRef ref = std::make_shared<ColumnNullable>(nested, nulls);
    auto w3 = TestNullable::Wrap(ref);
    EXPECT_EQ(w3->At(0), std::optional<uint64_t>(7));
    EXPECT_NE(ref, nullptr);
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

TEST(ColumnsCase, ColumnTupleT_Wrap_DoesNotStealSource) {
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
    auto wrapped = TestTuple::Wrap(std::move(col));

    // Wrapper sees the same data.
    EXPECT_EQ(wrapped->Size(), 1u);
    EXPECT_EQ(val, wrapped->At(0));

    // Source column is left intact after Wrap (non-stealing).
    EXPECT_EQ(col.TupleSize(), 3u);
    EXPECT_EQ(col.Size(), 1u);

    // Storage is shared: appending through the original element columns is visible via the wrapper.
    col[0]->AsStrict<ColumnUInt64>()->Append(2);
    col[1]->AsStrict<ColumnString>()->Append("xy");
    col[2]->AsStrict<ColumnFixedString>()->Append("zzz");
    EXPECT_EQ(wrapped->Size(), 2u);
    EXPECT_EQ(std::make_tuple(2, "xy", "zzz"), wrapped->At(1));
}

TEST(ColumnsCase, ColumnTupleT_Wrap_DoesNotStealSource_PreservesNames) {
    ColumnTuple base(
        {std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()},
        {"id", "name"}
    );

    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;
    auto wrapped = TestTuple::Wrap(std::move(base));
    EXPECT_EQ(wrapped->Type()->GetName(), "Tuple(id UInt64, name String)");

    // Source remains usable after Wrap (non-stealing).
    EXPECT_EQ(base.Type()->GetName(), "Tuple(id UInt64, name String)");
    EXPECT_EQ(base.TupleSize(), 2u);
}

TEST(ColumnsCase, ColumnTupleT_Wrap_AcceptsLvalue) {
    ColumnTuple col({
        std::make_shared<ColumnUInt64>(),
        std::make_shared<ColumnString>()
    });
    col[0]->AsStrict<ColumnUInt64>()->Append(1);
    col[1]->AsStrict<ColumnString>()->Append("a");

    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;

    // Non-const lvalue concrete column, no std::move required.
    auto w1 = TestTuple::Wrap(col);
    EXPECT_EQ(w1->At(0), std::make_tuple(uint64_t(1), std::string_view("a")));

    // Const lvalue concrete column.
    const ColumnTuple& cref = col;
    auto w2 = TestTuple::Wrap(cref);
    EXPECT_EQ(w2->At(0), std::make_tuple(uint64_t(1), std::string_view("a")));

    // Lvalue ColumnRef, no std::move required and not consumed.
    ColumnRef ref = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{col[0], col[1]});
    auto w3 = TestTuple::Wrap(ref);
    EXPECT_EQ(w3->At(0), std::make_tuple(uint64_t(1), std::string_view("a")));
    EXPECT_NE(ref, nullptr);

    // Source column left intact.
    EXPECT_EQ(col.TupleSize(), 2u);
    EXPECT_EQ(col.Size(), 1u);
}

TEST(ColumnsCase, ColumnTupleT_Empty) {
    using TestTuple = ColumnTupleT<>;

    TestTuple col(std::make_tuple());
    const auto val = std::make_tuple();
    col.Append(val);
    EXPECT_EQ(col.Size(), 0u);
}

TEST(ColumnsCase, ColumnTupleT_WithNames) {
    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;

    TestTuple col(
        std::make_tuple(
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>()
        ),
        std::vector<std::string>{"id", "name"}
    );
    EXPECT_EQ(col.Type()->GetName(), "Tuple(id UInt64, name String)");

    col.Append(std::make_tuple(uint64_t(42), std::string("hello")));
    EXPECT_EQ(col.At(0), std::make_tuple(uint64_t(42), std::string_view("hello")));
}

TEST(ColumnsCase, ColumnTupleT_Wrap_PreservesNames) {
    ColumnTuple base(
        {std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()},
        {"id", "name"}
    );

    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;
    auto wrapped = TestTuple::Wrap(std::move(base));
    EXPECT_EQ(wrapped->Type()->GetName(), "Tuple(id UInt64, name String)");
}

// --- Swap/Clear must stay coherent with As<>/Wrap views (contents swapped/cleared in place) ---

TEST(ColumnsCase, ColumnArrayT_Swap_VisibleThroughAlias) {
    auto a = std::make_shared<ColumnArrayT<ColumnUInt64>>();
    a->Append(std::vector<uint64_t>{1, 2, 3});
    auto b = std::make_shared<ColumnArrayT<ColumnUInt64>>();
    b->Append(std::vector<uint64_t>{7, 8});

    // Alias sharing a's storage.
    auto alias_a = ColumnArrayT<ColumnUInt64>::Wrap(a);
    ASSERT_NE(alias_a, nullptr);

    a->Swap(*b);

    // a now holds b's row, b holds a's row.
    ASSERT_EQ(a->Size(), 1u);
    EXPECT_EQ(a->At(0).size(), 2u);
    ASSERT_EQ(b->Size(), 1u);
    EXPECT_EQ(b->At(0).size(), 3u);

    // The swap is visible through the alias (sub-object identity preserved).
    ASSERT_EQ(alias_a->Size(), 1u);
    EXPECT_EQ(alias_a->At(0).size(), 2u);
    EXPECT_EQ(alias_a->At(0)[0], 7u);
}

TEST(ColumnsCase, ColumnNullableT_Swap_VisibleThroughAlias) {
    auto a = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    a->Append(1);
    a->Append(std::nullopt);
    auto b = std::make_shared<ColumnNullableT<ColumnUInt64>>();
    b->Append(42);

    auto alias_a = ColumnNullableT<ColumnUInt64>::Wrap(a);
    ASSERT_NE(alias_a, nullptr);

    a->Swap(*b);

    ASSERT_EQ(a->Size(), 1u);
    EXPECT_EQ(a->At(0), std::optional<uint64_t>(42));
    ASSERT_EQ(b->Size(), 2u);

    // Visible through the alias.
    ASSERT_EQ(alias_a->Size(), 1u);
    EXPECT_EQ(alias_a->At(0), std::optional<uint64_t>(42));
}

TEST(ColumnsCase, ColumnTupleT_Swap_VisibleThroughAlias) {
    ColumnTuple a({std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()});
    a[0]->AsStrict<ColumnUInt64>()->Append(1);
    a[1]->AsStrict<ColumnString>()->Append("a");

    ColumnTuple b({std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()});
    b[0]->AsStrict<ColumnUInt64>()->Append(2);
    b[1]->AsStrict<ColumnString>()->Append("b");

    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;
    auto alias_a = TestTuple::Wrap(a);
    ASSERT_NE(alias_a, nullptr);

    a.Swap(b);

    EXPECT_EQ(a.Size(), 1u);
    EXPECT_EQ(a[0]->AsStrict<ColumnUInt64>()->At(0), 2u);
    EXPECT_EQ(b[0]->AsStrict<ColumnUInt64>()->At(0), 1u);

    // Visible through the alias.
    EXPECT_EQ(alias_a->At(0), std::make_tuple(uint64_t(2), std::string_view("b")));
}

TEST(ColumnsCase, ColumnTuple_Swap_DifferentSizeThrows) {
    ColumnTuple a({std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()});
    ColumnTuple b({std::make_shared<ColumnUInt64>()});
    EXPECT_THROW(a.Swap(b), ValidationError);
}

TEST(ColumnsCase, ColumnTuple_Swap_DifferentElementTypeDoesNotModify) {
    auto a0 = std::make_shared<ColumnUInt64>();
    auto a1 = std::make_shared<ColumnString>();
    a0->Append(1);
    a1->Append("a");
    ColumnTuple a({a0, a1});

    auto b0 = std::make_shared<ColumnUInt64>();
    auto b1 = std::make_shared<ColumnUInt64>();
    b0->Append(2);
    b1->Append(3);
    ColumnTuple b({b0, b1});

    EXPECT_THROW(a.Swap(b), ValidationError);

    EXPECT_EQ(a0->At(0), 1u);
    EXPECT_EQ(a1->At(0), "a");
    EXPECT_EQ(b0->At(0), 2u);
    EXPECT_EQ(b1->At(0), 3u);
}

TEST(ColumnsCase, ColumnTuple_Clear_PreservesStructure_AndAlias) {
    ColumnTuple col({std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()});
    col[0]->AsStrict<ColumnUInt64>()->Append(1);
    col[1]->AsStrict<ColumnString>()->Append("a");

    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;
    auto alias = TestTuple::Wrap(col);
    ASSERT_NE(alias, nullptr);
    ASSERT_EQ(alias->Size(), 1u);

    col.Clear();

    // Structure is preserved (columns not dropped) and data is cleared in place.
    EXPECT_EQ(col.Size(), 0u);
    EXPECT_EQ(col.TupleSize(), 2u);
    // Clear propagates to the alias.
    EXPECT_EQ(alias->Size(), 0u);

    // Re-appending through the original element columns is visible via the alias.
    col[0]->AsStrict<ColumnUInt64>()->Append(2);
    col[1]->AsStrict<ColumnString>()->Append("b");
    ASSERT_EQ(alias->Size(), 1u);
    EXPECT_EQ(alias->At(0), std::make_tuple(uint64_t(2), std::string_view("b")));
}

TEST(ColumnsCase, ColumnMapT_Swap_VisibleThroughAlias) {
    using TestMap = ColumnMapT<ColumnString, ColumnUInt64>;

    auto a = std::make_shared<TestMap>(std::make_shared<ColumnString>(), std::make_shared<ColumnUInt64>());
    a->Append(std::map<std::string, uint64_t>{{"x", 1}});
    auto b = std::make_shared<TestMap>(std::make_shared<ColumnString>(), std::make_shared<ColumnUInt64>());
    b->Append(std::map<std::string, uint64_t>{{"y", 2}, {"z", 3}});

    auto alias_a = TestMap::Wrap(a);
    ASSERT_NE(alias_a, nullptr);

    a->Swap(*b);

    ASSERT_EQ(a->Size(), 1u);
    EXPECT_EQ(a->At(0).size(), 2u);
    ASSERT_EQ(b->Size(), 1u);
    EXPECT_EQ(b->At(0).size(), 1u);

    // Visible through the alias.
    ASSERT_EQ(alias_a->Size(), 1u);
    EXPECT_EQ(alias_a->At(0).size(), 2u);
    EXPECT_EQ(alias_a->At(0)["y"], 2u);
}

// --- Deep-nested aliases: in-place Swap/Clear must recurse to shared leaf objects ---

namespace {
// Builds a single-row Map(UInt64, Array(Nullable(String))): one map row with one entry
// {key -> arr}. Uses the single-offset ColumnArray ctor so the backing array has exactly one row.
std::shared_ptr<ColumnMap> MakeDeepMapRow(uint64_t key, std::vector<std::optional<std::string>> arr) {
    auto keys = std::make_shared<ColumnUInt64>();
    auto vals = std::make_shared<ColumnArrayT<ColumnNullableT<ColumnString>>>();
    keys->Append(key);
    vals->Append(arr);
    auto tuple = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{keys, vals});
    return std::make_shared<ColumnMap>(std::make_shared<ColumnArray>(tuple));
}
}

TEST(ColumnsCase, DeepMap_Swap_VisibleThroughAlias) {
    using DeepMap = ColumnMapT<ColumnUInt64, ColumnArrayT<ColumnNullableT<ColumnString>>>;

    auto a = MakeDeepMapRow(1, {std::string("a"), std::string("b"), std::string("c")});
    auto b = MakeDeepMapRow(1, {std::string("x"), std::nullopt});

    auto alias_a = DeepMap::Wrap(a);  // deep typed view created BEFORE the swap
    ASSERT_NE(alias_a, nullptr);
    ASSERT_EQ(alias_a->At(0).At(1).Size(), 3u);

    a->Swap(*b);

    // The swap recurses down to the shared leaf columns, so the pre-existing alias now
    // reflects B's data: the value-array flips size 3 -> 2 and the null survives.
    auto arr = alias_a->At(0).At(1);
    ASSERT_EQ(arr.Size(), 2u);
    EXPECT_EQ(arr[0], std::optional<std::string_view>("x"));
    EXPECT_EQ(arr[1], std::optional<std::string_view>{});

    // b now holds a's original data.
    auto arr_b = DeepMap::Wrap(b)->At(0).At(1);
    EXPECT_EQ(arr_b.Size(), 3u);
    EXPECT_EQ(arr_b[0], std::optional<std::string_view>("a"));
}

TEST(ColumnsCase, DeepMap_Clear_VisibleThroughAlias_NoStaleEntries) {
    using DeepMap = ColumnMapT<ColumnUInt64, ColumnArrayT<ColumnNullableT<ColumnString>>>;

    auto a = MakeDeepMapRow(1, {std::string("a"), std::string("b"), std::string("c")});
    auto alias_a = DeepMap::Wrap(a);
    ASSERT_NE(alias_a, nullptr);
    ASSERT_EQ(alias_a->Size(), 1u);

    a->Clear();

    // Clear recurses to the shared leaves in place, so the alias goes empty too.
    EXPECT_EQ(a->Size(), 0u);
    EXPECT_EQ(alias_a->Size(), 0u);

    // Re-appending a fresh row must not resurface the pre-clear ["a","b","c"] entry.
    a->Append(MakeDeepMapRow(1, {std::string("z")}));
    ASSERT_EQ(alias_a->Size(), 1u);
    auto arr = alias_a->At(0).At(1);
    ASSERT_EQ(arr.Size(), 1u);
    EXPECT_EQ(arr[0], std::optional<std::string_view>("z"));
}

TEST(ColumnsCase, DeepNestedArray_Swap_VisibleThroughAlias) {
    using DeepArray = ColumnArrayT<ColumnArrayT<ColumnNullableT<ColumnString>>>;

    auto a = std::make_shared<DeepArray>();
    a->Append(std::vector<std::vector<std::optional<std::string>>>{
        {std::string("a")}, {std::string("b"), std::string("c")}});
    auto b = std::make_shared<DeepArray>();
    b->Append(std::vector<std::vector<std::optional<std::string>>>{
        {std::string("x"), std::nullopt}});

    auto alias_a = DeepArray::Wrap(a);  // created BEFORE the swap
    ASSERT_NE(alias_a, nullptr);
    ASSERT_EQ(alias_a->At(0).Size(), 2u);

    a->Swap(*b);

    // Alias reflects B's data all the way down to the nullable-string leaves.
    auto outer = alias_a->At(0);
    ASSERT_EQ(outer.Size(), 1u);
    auto inner = outer.At(0);
    ASSERT_EQ(inner.Size(), 2u);
    EXPECT_EQ(inner[0], std::optional<std::string_view>("x"));
    EXPECT_EQ(inner[1], std::optional<std::string_view>{});
}

// --- Deep nesting with LowCardinality: Map(UInt64, Array(LowCardinality(Nullable(String)))) ---

namespace {
// Builds a single-row Map(UInt64, Array(LowCardinality(Nullable(String)))): one map row with one
// entry {key -> arr}, where the value is an array of LowCardinality(Nullable(String)) items.
std::shared_ptr<ColumnMap> MakeDeepLcMapRow(uint64_t key, std::vector<std::optional<std::string>> arr) {
    auto keys = std::make_shared<ColumnUInt64>();
    auto vals = std::make_shared<ColumnArrayT<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>>();
    keys->Append(key);
    vals->Append(arr);
    auto tuple = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{keys, vals});
    return std::make_shared<ColumnMap>(std::make_shared<ColumnArray>(tuple));
}
}

TEST(ColumnsCase, DeepMapArrayLowCardinality_Swap_VisibleThroughAlias) {
    using DeepLcMap = ColumnMapT<ColumnUInt64, ColumnArrayT<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>>;

    auto a = MakeDeepLcMapRow(1, {std::string("a"), std::string("b"), std::string("c")});
    auto b = MakeDeepLcMapRow(1, {std::string("x"), std::nullopt});

    auto alias_a = DeepLcMap::Wrap(a);  // deep typed view created BEFORE the swap
    ASSERT_NE(alias_a, nullptr);
    ASSERT_EQ(alias_a->At(0).At(1).Size(), 3u);

    a->Swap(*b);

    // The swap recurses through Array -> LowCardinality (dictionary swapped in place, index bundle
    // swapped) down to the Nullable(String) leaves, so the pre-existing alias reflects B's data:
    // the value-array flips size 3 -> 2 and the null survives through LC -> Nullable.
    auto arr = alias_a->At(0).At(1);
    ASSERT_EQ(arr.Size(), 2u);
    EXPECT_EQ(arr[0], std::optional<std::string_view>("x"));
    EXPECT_EQ(arr[1], std::optional<std::string_view>{});

    // b now holds a's original data.
    auto arr_b = DeepLcMap::Wrap(b)->At(0).At(1);
    ASSERT_EQ(arr_b.Size(), 3u);
    EXPECT_EQ(arr_b[0], std::optional<std::string_view>("a"));
}

TEST(ColumnsCase, DeepMapArrayLowCardinality_Clear_VisibleThroughAlias) {
    using DeepLcMap = ColumnMapT<ColumnUInt64, ColumnArrayT<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>>;

    auto a = MakeDeepLcMapRow(1, {std::string("a"), std::string("b"), std::string("c")});
    auto alias_a = DeepLcMap::Wrap(a);
    ASSERT_NE(alias_a, nullptr);
    ASSERT_EQ(alias_a->Size(), 1u);

    a->Clear();

    // Clear recurses to the shared leaves in place (incl. LowCardinality), so the alias empties too.
    EXPECT_EQ(a->Size(), 0u);
    EXPECT_EQ(alias_a->Size(), 0u);

    // Re-appending a fresh row must not resurface the pre-clear entry.
    a->Append(MakeDeepLcMapRow(1, {std::string("z")}));
    ASSERT_EQ(alias_a->Size(), 1u);
    auto arr = alias_a->At(0).At(1);
    ASSERT_EQ(arr.Size(), 1u);
    EXPECT_EQ(arr[0], std::optional<std::string_view>("z"));
}

// --- const As()/AsStrict() wrap like their non-const counterparts ---

TEST(ColumnsCase, Const_As_WrapsWrappableColumn) {
    using TestArray = ColumnArrayT<ColumnUInt64>;

    auto arr = std::make_shared<TestArray>();
    arr->Append(std::vector<uint64_t>{1, 2, 3});

    // View the column only through a const handle.
    std::shared_ptr<const Column> c = arr;
    auto view = c->As<TestArray>();
    ASSERT_NE(view, nullptr);
    static_assert(std::is_same_v<decltype(view), std::shared_ptr<const TestArray>>,
                  "const As() must yield shared_ptr<const T>");

    // Read access reflects the same shared storage.
    ASSERT_EQ(view->Size(), 1u);
    auto row = view->At(0);
    ASSERT_EQ(row.Size(), 3u);
    EXPECT_EQ(row[0], 1u);
    EXPECT_EQ(row[2], 3u);
}

TEST(ColumnsCase, Const_As_ExactDowncastStillWorks) {
    auto leaf = std::make_shared<ColumnUInt64>();
    leaf->Append(42u);

    std::shared_ptr<const Column> c = leaf;
    auto exact = c->As<ColumnUInt64>();
    ASSERT_NE(exact, nullptr);
    ASSERT_EQ(exact->Size(), 1u);
    EXPECT_EQ(exact->At(0), 42u);

    // Non-wrappable mismatch returns nullptr (no throw).
    EXPECT_EQ(c->As<ColumnString>(), nullptr);
}

TEST(ColumnsCase, Const_AsStrict_WrapsAndThrows) {
    using TestArray = ColumnArrayT<ColumnUInt64>;

    auto arr = std::make_shared<TestArray>();
    arr->Append(std::vector<uint64_t>{7, 8});

    std::shared_ptr<const Column> c = arr;
    auto view = c->AsStrict<TestArray>();
    ASSERT_NE(view, nullptr);
    static_assert(std::is_same_v<decltype(view), std::shared_ptr<const TestArray>>,
                  "const AsStrict() must yield shared_ptr<const T>");
    ASSERT_EQ(view->At(0).Size(), 2u);

    // Wrappable-but-incompatible element type throws.
    EXPECT_THROW((void)c->AsStrict<ColumnArrayT<ColumnString>>(), ValidationError);
    // Non-wrappable mismatch throws too.
    EXPECT_THROW((void)c->AsStrict<ColumnString>(), ValidationError);
}

TEST(ColumnsCase, ColumnTupleT_Slice_PreservesNames) {
    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;

    auto col = std::make_shared<TestTuple>(
        std::make_tuple(
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>()
        ),
        std::vector<std::string>{"id", "name"}
    );
    col->Append(std::make_tuple(uint64_t(1), std::string("a")));
    col->Append(std::make_tuple(uint64_t(2), std::string("b")));

    auto sliced = col->Slice(0, 1);
    EXPECT_EQ(sliced->Type()->GetName(), "Tuple(id UInt64, name String)");

    auto cloned = col->CloneEmpty();
    EXPECT_EQ(cloned->Type()->GetName(), "Tuple(id UInt64, name String)");
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

// Regression tests for general LowCardinality support over non-String inner
// types (previously only String/FixedString were supported).
TEST(ColumnLowCardinality, AppendAndReadNumeric) {
    auto col = std::make_shared<ColumnLowCardinalityT<ColumnInt64>>();
    col->Append(7);
    col->Append(7);
    col->Append(9);
    col->Append(7);

    ASSERT_EQ(4u, col->Size());
    EXPECT_EQ(7, col->At(0));
    EXPECT_EQ(7, col->At(1));
    EXPECT_EQ(9, col->At(2));
    EXPECT_EQ(7, col->At(3));
    // Dictionary holds the default item plus the two distinct values {7, 9}.
    EXPECT_EQ(3u, col->GetDictionarySize());

    // GetItem returns the raw value with the correct type code.
    const auto item = col->GetItem(2);
    EXPECT_EQ(Type::Int64, item.type);
    EXPECT_EQ(9, item.get<int64_t>());
}

TEST(ColumnLowCardinality, AppendAndReadNullableNumeric) {
    auto col
        = std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnInt64>>>();
    col->Append(7);
    col->Append(std::nullopt);
    col->Append(7);
    col->Append(9);
    col->Append(std::nullopt);

    ASSERT_EQ(5u, col->Size());
    EXPECT_EQ(std::optional<int64_t>{7}, col->At(0));
    EXPECT_EQ(std::nullopt, col->At(1));
    EXPECT_EQ(std::optional<int64_t>{7}, col->At(2));
    EXPECT_EQ(std::optional<int64_t>{9}, col->At(3));
    EXPECT_EQ(std::nullopt, col->At(4));

    // Null rows are represented by a Void ItemView.
    EXPECT_EQ(Type::Void, col->GetItem(1).type);
    EXPECT_EQ(Type::Int64, col->GetItem(0).type);
}

TEST(ColumnLowCardinality, NumericLoadAndSave) {
    auto column_A = std::make_shared<ColumnLowCardinalityT<ColumnUInt64>>();
    for (auto v : {1u, 2u, 1u, 3u, 2u, 1u}) {
        column_A->Append(v);
    }

    const auto BufferSize = 64 * 1024;
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(BufferSize);
    memset(buffer.get(), 0, BufferSize);
    {
        ArrayOutput output(buffer.get(), BufferSize);
        ASSERT_NO_THROW(column_A->Save(&output));
    }

    auto column_B = std::make_shared<ColumnLowCardinalityT<ColumnUInt64>>();
    {
        ArrayInput input(buffer.get(), BufferSize);
        ASSERT_TRUE(column_B->Load(&input, column_A->Size()));
    }

    ASSERT_EQ(column_A->Size(), column_B->Size());
    for (size_t i = 0; i < column_A->Size(); ++i) {
        EXPECT_EQ(column_A->At(i), column_B->At(i)) << "row " << i;
    }
}

TEST(ColumnsCase, ColumnMapT_Wrap_AcceptsLvalue) {
    auto tupls = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>()});

    auto data = std::make_shared<ColumnArray>(tupls);

    auto val = tupls->CloneEmpty()->As<ColumnTuple>();
    (*val)[0]->AsStrict<ColumnUInt64>()->Append(1);
    (*val)[1]->AsStrict<ColumnString>()->Append("123");
    data->AppendAsColumn(val);

    ColumnMap col{data};

    using TestMap = ColumnMapT<ColumnUInt64, ColumnString>;

    // Non-const lvalue concrete column, no std::move required.
    auto w1 = TestMap::Wrap(col);
    EXPECT_EQ("123", w1->At(0).At(1));

    // Const lvalue concrete column.
    const ColumnMap& cref = col;
    auto w2 = TestMap::Wrap(cref);
    EXPECT_EQ("123", w2->At(0).At(1));

    // Lvalue ColumnRef, no std::move required and not consumed.
    ColumnRef ref = std::make_shared<ColumnMap>(data);
    auto w3 = TestMap::Wrap(ref);
    EXPECT_EQ("123", w3->At(0).At(1));
    EXPECT_NE(ref, nullptr);

    // Source column left intact.
    EXPECT_EQ(col.Size(), 1u);
}

TEST(ColumnsCase, ColumnMapT_Wrap_DoesNotStealSource) {
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

    // Wrapper sees the same data.
    auto map_view = wrapped_col->At(0);
    EXPECT_THROW(map_view.At(0), ValidationError);
    EXPECT_EQ("123", map_view.At(1));
    EXPECT_EQ("abc", map_view.At(2));

    // Source column is left intact after Wrap (non-stealing).
    EXPECT_EQ(col.Size(), 1u);

    // Storage is shared: appending a row through the original is visible via the wrapper.
    auto val2 = tupls->CloneEmpty()->As<ColumnTuple>();
    (*val2)[0]->AsStrict<ColumnUInt64>()->Append(7);
    (*val2)[1]->AsStrict<ColumnString>()->Append("xyz");
    data->AppendAsColumn(val2);

    EXPECT_EQ(wrapped_col->Size(), 2u);
    EXPECT_EQ("xyz", wrapped_col->At(1).At(7));
}

// --- Wrap error-reporting overloads ---------------------------------------------------------
// The two-argument Wrap(col, ValidationError*) returns nullptr (never throws) on a type
// mismatch; the single-argument Wrap(col) throws ValidationError on the same mismatch.

TEST(ColumnsCase, ColumnArrayT_Wrap_TypeMismatch) {
    using TestArray = ColumnArrayT<ColumnUInt64>;

    // Right kind (Array), wrong element type (String instead of UInt64).
    ColumnRef bad_element = std::make_shared<ColumnArray>(std::make_shared<ColumnString>());
    // Wrong kind entirely.
    ColumnRef not_array = std::make_shared<ColumnUInt64>();

    ValidationError error;
    EXPECT_NO_THROW({
        EXPECT_EQ(TestArray::Wrap(bad_element, &error), nullptr);
    });
    EXPECT_FALSE(std::string_view(error.what()).empty());

    // Passing nullptr for the error is allowed and still non-throwing.
    EXPECT_NO_THROW({
        EXPECT_EQ(TestArray::Wrap(not_array, nullptr), nullptr);
    });

    // Single-argument overload throws on the same mismatches.
    EXPECT_THROW(TestArray::Wrap(bad_element), ValidationError);
    EXPECT_THROW(TestArray::Wrap(not_array), ValidationError);

    // Sanity: a matching column wraps fine through both overloads.
    ColumnRef good = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
    EXPECT_NE(TestArray::Wrap(good, nullptr), nullptr);
    EXPECT_NE(TestArray::Wrap(good), nullptr);
}

TEST(ColumnsCase, ColumnNullableT_Wrap_TypeMismatch) {
    using TestNullable = ColumnNullableT<ColumnUInt64>;

    // Nullable of the wrong nested type.
    ColumnRef bad_nested = std::make_shared<ColumnNullable>(
        std::make_shared<ColumnString>(), std::make_shared<ColumnUInt8>());
    ColumnRef not_nullable = std::make_shared<ColumnUInt64>();

    ValidationError error;
    EXPECT_EQ(TestNullable::Wrap(bad_nested, &error), nullptr);
    EXPECT_FALSE(std::string_view(error.what()).empty());
    EXPECT_EQ(TestNullable::Wrap(not_nullable, nullptr), nullptr);

    EXPECT_THROW(TestNullable::Wrap(bad_nested), ValidationError);
    EXPECT_THROW(TestNullable::Wrap(not_nullable), ValidationError);
}

TEST(ColumnsCase, ColumnTupleT_Wrap_TypeMismatch) {
    using TestTuple = ColumnTupleT<ColumnUInt64, ColumnString>;

    // Correct arity, wrong element type.
    ColumnRef bad_element = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        std::make_shared<ColumnUInt64>(), std::make_shared<ColumnUInt64>()});
    // Wrong arity.
    ColumnRef bad_arity = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        std::make_shared<ColumnUInt64>()});
    // Wrong kind.
    ColumnRef not_tuple = std::make_shared<ColumnUInt64>();

    ValidationError error;
    EXPECT_EQ(TestTuple::Wrap(bad_element, &error), nullptr);
    EXPECT_FALSE(std::string_view(error.what()).empty());
    EXPECT_EQ(TestTuple::Wrap(bad_arity, nullptr), nullptr);
    EXPECT_EQ(TestTuple::Wrap(not_tuple, nullptr), nullptr);

    EXPECT_THROW(TestTuple::Wrap(bad_element), ValidationError);
    EXPECT_THROW(TestTuple::Wrap(bad_arity), ValidationError);
    EXPECT_THROW(TestTuple::Wrap(not_tuple), ValidationError);
}

TEST(ColumnsCase, ColumnMapT_Wrap_TypeMismatch) {
    using TestMap = ColumnMapT<ColumnUInt64, ColumnString>;
    ColumnRef not_map = std::make_shared<ColumnUInt64>();

    ValidationError error;
    EXPECT_EQ(TestMap::Wrap(not_map, &error), nullptr);
    EXPECT_FALSE(std::string_view(error.what()).empty());
    EXPECT_EQ(TestMap::Wrap(not_map, nullptr), nullptr);
    EXPECT_THROW(TestMap::Wrap(not_map), ValidationError);
}

TEST(ColumnsCase, ColumnLowCardinalityT_Wrap_TypeMismatch) {
    using TestLC = ColumnLowCardinalityT<ColumnString>;

    // LowCardinality with the wrong (but valid) dictionary type.
    ColumnRef bad_dict = std::make_shared<ColumnLowCardinality>(std::make_shared<ColumnFixedString>(4));
    ColumnRef not_lc = std::make_shared<ColumnUInt64>();

    ValidationError error;
    EXPECT_EQ(TestLC::Wrap(bad_dict, &error), nullptr);
    EXPECT_FALSE(std::string_view(error.what()).empty());
    EXPECT_EQ(TestLC::Wrap(not_lc, nullptr), nullptr);

    EXPECT_THROW(TestLC::Wrap(bad_dict), ValidationError);
    EXPECT_THROW(TestLC::Wrap(not_lc), ValidationError);
}
