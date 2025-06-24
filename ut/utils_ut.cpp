#include <gtest/gtest.h>
#include "clickhouse/base/uuid.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/enum.h"
#include "clickhouse/columns/ip4.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/uuid.h"
#include "ut/value_generators.h"
#include "utils.h"
#include "absl/numeric/int128.h"

#include <limits>
#include <optional>
#include <sstream>
#include <vector>

TEST(CompareRecursive, CompareValues) {
    EXPECT_TRUE(CompareRecursive(1, 1));
    EXPECT_TRUE(CompareRecursive(1.0f, 1.0f));
    EXPECT_TRUE(CompareRecursive(1.0, 1.0));
    EXPECT_TRUE(CompareRecursive(1.0L, 1.0L));

    EXPECT_TRUE(CompareRecursive('a', 'a'));
}

TEST(CompareRecursive, CompareStrings) {
    // literals
    EXPECT_TRUE(CompareRecursive("1.0L", "1.0L"));
    EXPECT_TRUE(CompareRecursive("1.0L", std::string("1.0L")));
    EXPECT_TRUE(CompareRecursive(std::string("1.0L"), "1.0L"));
    EXPECT_TRUE(CompareRecursive("1.0L", std::string_view("1.0L")));
    EXPECT_TRUE(CompareRecursive(std::string_view("1.0L"), "1.0L"));

    // array
    const char str[] = "1.0L";
    EXPECT_TRUE(CompareRecursive("1.0L", str));
    EXPECT_TRUE(CompareRecursive(str, "1.0L"));
    EXPECT_TRUE(CompareRecursive(str, str));
    EXPECT_TRUE(CompareRecursive(str, std::string("1.0L")));
    EXPECT_TRUE(CompareRecursive(std::string("1.0L"), str));
    EXPECT_TRUE(CompareRecursive(str, std::string_view("1.0L")));
    EXPECT_TRUE(CompareRecursive(std::string_view("1.0L"), str));

    // pointer
    const char *str2 = "1.0L";
    EXPECT_TRUE(CompareRecursive("1.0L", str2));
    EXPECT_TRUE(CompareRecursive(str2, "1.0L"));
    EXPECT_TRUE(CompareRecursive(str2, str2));
    EXPECT_TRUE(CompareRecursive(str2, str));
    EXPECT_TRUE(CompareRecursive(str, str2));
    EXPECT_TRUE(CompareRecursive(str2, std::string("1.0L")));
    EXPECT_TRUE(CompareRecursive(std::string("1.0L"), str2));
    EXPECT_TRUE(CompareRecursive(str2, std::string_view("1.0L")));
    EXPECT_TRUE(CompareRecursive(std::string_view("1.0L"), str2));

    // string & string_view
    EXPECT_TRUE(CompareRecursive(std::string{"1.0L"},      std::string{"1.0L"}));
    EXPECT_TRUE(CompareRecursive(std::string_view{"1.0L"}, std::string_view{"1.0L"}));
    EXPECT_TRUE(CompareRecursive(std::string{"1.0L"},      std::string_view{"1.0L"}));
    EXPECT_TRUE(CompareRecursive(std::string_view{"1.0L"}, std::string{"1.0L"}));
}

TEST(CompareRecursive, CompareContainerOfStrings) {
    const std::vector<const char*> vector_of_cstrings = {
        "abc",
        "cde",
        "ghi"
    };

    const std::vector<std::string> vector_of_strings = {
        "abc",
        "cde",
        "ghi"
    };

    const std::vector<std::string_view> vector_of_string_views = {
        "abc",
        "cde",
        "ghi"
    };

    {
        // same values, but different pointers
        const std::vector<const char*> vector_of_cstrings2 = {
            vector_of_strings[0].data(),
            vector_of_strings[1].data(),
            vector_of_strings[2].data(),
        };
        EXPECT_TRUE(CompareRecursive(vector_of_cstrings, vector_of_cstrings2));
    }

    EXPECT_TRUE(CompareRecursive(vector_of_strings, vector_of_strings));
    EXPECT_TRUE(CompareRecursive(vector_of_strings, vector_of_cstrings));
    EXPECT_TRUE(CompareRecursive(vector_of_cstrings, vector_of_strings));

    EXPECT_TRUE(CompareRecursive(vector_of_string_views, vector_of_string_views));
    EXPECT_TRUE(CompareRecursive(vector_of_strings, vector_of_string_views));
    EXPECT_TRUE(CompareRecursive(vector_of_string_views, vector_of_strings));
    EXPECT_TRUE(CompareRecursive(vector_of_strings, vector_of_string_views));
    EXPECT_TRUE(CompareRecursive(vector_of_string_views, vector_of_strings));
}

TEST(CompareRecursive, CompareContainers) {
    EXPECT_TRUE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 3}));
    EXPECT_TRUE(CompareRecursive(std::vector<int>{}, std::vector<int>{}));

    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 4}));
    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2}));

    // That would cause compile-time error:
    // EXPECT_FALSE(CompareRecursive(std::array{1, 2, 3}, 1));
}


TEST(CompareRecursive, CompareNestedContainers) {
    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}}},
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}}}));

    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}, {}}},
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}, {}}}));

    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{}}},
        std::vector<std::vector<int>>{{{}}}));

    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 7}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {4, 5}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{}}));
}

TEST(StringUtils, UUID) {
    const clickhouse::UUID& uuid{0x0102030405060708, 0x090a0b0c0d0e0f10};
    const std::string uuid_string = "01020304-0506-0708-090a-0b0c0d0e0f10";
    EXPECT_EQ(ToString(uuid), uuid_string);
}

TEST(CompareRecursive, Nan) {
    /// Even though NaN == NaN is FALSE, CompareRecursive must compare those as TRUE.

    const auto NaNf = std::numeric_limits<float>::quiet_NaN();
    const auto NaNd = std::numeric_limits<double>::quiet_NaN();

    EXPECT_TRUE(CompareRecursive(NaNf, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNd));

    EXPECT_TRUE(CompareRecursive(NaNf, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNf));

    // 1.0 is arbitrary here
    EXPECT_FALSE(CompareRecursive(NaNf, 1.0));
    EXPECT_FALSE(CompareRecursive(NaNf, 1.0));
    EXPECT_FALSE(CompareRecursive(1.0, NaNd));
    EXPECT_FALSE(CompareRecursive(1.0, NaNd));
}

TEST(CompareRecursive, Optional) {
    EXPECT_TRUE(CompareRecursive(1, std::optional{1}));
    EXPECT_TRUE(CompareRecursive(std::optional{1}, 1));
    EXPECT_TRUE(CompareRecursive(std::optional{1}, std::optional{1}));

    EXPECT_FALSE(CompareRecursive(2, std::optional{1}));
    EXPECT_FALSE(CompareRecursive(std::optional{1}, 2));
    EXPECT_FALSE(CompareRecursive(std::optional{2}, std::optional{1}));
    EXPECT_FALSE(CompareRecursive(std::optional{1}, std::optional{2}));
}

TEST(CompareRecursive, OptionalNan) {
    // Special case for optional comparison:
    // NaNs should be considered as equal (compare by unpacking value of optional)

    const auto NaNf = std::numeric_limits<float>::quiet_NaN();
    const auto NaNd = std::numeric_limits<double>::quiet_NaN();

    const auto NaNfo = std::optional{NaNf};
    const auto NaNdo = std::optional{NaNd};

    EXPECT_TRUE(CompareRecursive(NaNf, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNf, NaNfo));
    EXPECT_TRUE(CompareRecursive(NaNfo, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNfo, NaNfo));

    EXPECT_TRUE(CompareRecursive(NaNd, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNdo));
    EXPECT_TRUE(CompareRecursive(NaNdo, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNdo, NaNdo));

    EXPECT_FALSE(CompareRecursive(NaNdo, std::optional<double>{}));
    EXPECT_FALSE(CompareRecursive(NaNfo, std::optional<float>{}));
    EXPECT_FALSE(CompareRecursive(std::optional<double>{}, NaNdo));
    EXPECT_FALSE(CompareRecursive(std::optional<float>{}, NaNfo));

    // Too lazy to comparison code against std::nullopt, but this shouldn't be a problem in real life
    // following will produce compile time error:
//    EXPECT_FALSE(CompareRecursive(NaNdo, std::nullopt));
//    EXPECT_FALSE(CompareRecursive(NaNfo, std::nullopt));
}


TEST(Generators, MakeArrays) {
    auto arrays = MakeArrays<std::string, MakeStrings>();
    ASSERT_LT(0u, arrays.size());
}

// I.e. object ItemView can be serialized to string
std::string toString(const clickhouse::ItemView & iv) {
    std::stringstream sstr;
    sstr << iv;

    return sstr.str();
}

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

#define EXPECTED_SERIALIZATION(expected, column_expression, value) \
do {\
    auto column = column_expression; \
    column.Append((value)); \
    EXPECT_EQ("ItemView {" expected "}", toString(column.GetItem(0))) \
        << "Created from " << STRINGIFY((value)); \
}\
while (false)

TEST(ItemView, OutputToOstream_VALID) {
    // Testing output of `std::ostream& operator<<(std::ostream& ostr, const ItemView& item_view)`
    // result must match predefined value.

    using namespace clickhouse;

    // Positive cases: output should be generated
    EXPECTED_SERIALIZATION("String : \"string\" (6 bytes)", ColumnString(), "string");
    EXPECTED_SERIALIZATION("FixedString : \"string\" (6 bytes)", ColumnFixedString(6), "string");

    EXPECTED_SERIALIZATION("Int8 : -123", ColumnInt8(), -123);
    EXPECTED_SERIALIZATION("Int16 : -1234", ColumnInt16(), -1234);
    EXPECTED_SERIALIZATION("Int32 : -12345", ColumnInt32(), -12345);
    EXPECTED_SERIALIZATION("Int64 : -123456", ColumnInt64(), -123456);
    EXPECTED_SERIALIZATION("Int128 : -123456789", ColumnInt128(), absl::MakeInt128(-1, -123456789ll));

    EXPECTED_SERIALIZATION("UInt8 : 123", ColumnUInt8(), 123);
    EXPECTED_SERIALIZATION("UInt16 : 1234", ColumnUInt16(), 1234);
    EXPECTED_SERIALIZATION("UInt32 : 12345", ColumnUInt32(), 12345);
    EXPECTED_SERIALIZATION("UInt64 : 1234567", ColumnUInt64(), 1234567);
    EXPECTED_SERIALIZATION("UInt128 : 123456789", ColumnUInt128(), absl::MakeUint128(0, 123456789ll));

    EXPECTED_SERIALIZATION("Float32 : 1", ColumnFloat32(), 1);
    EXPECTED_SERIALIZATION("Float64 : 4", ColumnFloat64(), 4);

    using EnumItem = Type::EnumItem;

    EXPECTED_SERIALIZATION("Enum8 : 123", ColumnEnum8(Type::CreateEnum8({EnumItem{"one", 123}})), 123);
    EXPECTED_SERIALIZATION("Enum16 : 12345", ColumnEnum16(Type::CreateEnum16({EnumItem{"one", 12345}})), 12345);

    EXPECTED_SERIALIZATION("IPv4 : 127.0.0.1", ColumnIPv4(), "127.0.0.1");
    EXPECTED_SERIALIZATION("IPv6 : ::ffff:204.152.189.116", ColumnIPv6(), "::ffff:204.152.189.116");

    EXPECTED_SERIALIZATION("UUID : bb6a8c69-9ab2-414c-8669-7b7fd27f0825", ColumnUUID(), UUID(0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu));

    EXPECTED_SERIALIZATION("Decimal : 1234567", ColumnDecimal(6, 0), 1234567);
    EXPECTED_SERIALIZATION("Decimal : 1234567", ColumnDecimal(6, 3), 1234567);
    EXPECTED_SERIALIZATION("Decimal : 1234567", ColumnDecimal(12, 0), 1234567);
    EXPECTED_SERIALIZATION("Decimal : 1234567", ColumnDecimal(12, 6), 1234567);
    EXPECTED_SERIALIZATION("Decimal : 1234567", ColumnDecimal(18, 0), 1234567);
    EXPECTED_SERIALIZATION("Decimal : 1234567", ColumnDecimal(18, 9), 1234567);

    EXPECTED_SERIALIZATION("Date : 1970-05-04 00:00:00", ColumnDate(), time_t(123) * 86400);
    EXPECTED_SERIALIZATION("DateTime : 1970-01-15 06:56:07", ColumnDateTime(), 1234567);
    // this is completely bogus, since precision is not taken into account
    EXPECTED_SERIALIZATION("DateTime64 : 1970-01-15 06:56:07", ColumnDateTime64(3), 1234567);
#if defined(_unix_)
    // These tests do not work on Windows, and since we test here auxiliary functionality
    // (not used by clients, but only in tests), I assume it is safe to just ignore the failure.
    EXPECTED_SERIALIZATION("DateTime64 : 1969-12-17 17:03:53", ColumnDateTime64(3), -1234567);

    {
        auto column = ColumnDate32();
        column.AppendRaw(-123);
        EXPECT_EQ("ItemView {Date32 : 1969-12-31 23:57:57}", toString(column.GetItem(0)));
    }
    // EXPECTED_SERIALIZATION("Date32 : 1969-08-31 00:00:00", ColumnDate32(), time_t(-123) * 86400);
#endif
}

namespace {

clickhouse::ItemView MakeEmptyItemView(clickhouse::Type::Code type_code) {
    return clickhouse::ItemView(type_code, std::string_view());
}

}

TEST(ItemView, OutputToOstream_negative) {
    using namespace clickhouse;

    // Doesn't matter what content we point ItemView into, those types are not supported.
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::LowCardinality)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Array)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Nullable)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Tuple)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Map)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Point)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Ring)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::Polygon)));
    EXPECT_ANY_THROW(toString(MakeEmptyItemView(Type::MultiPolygon)));

}
