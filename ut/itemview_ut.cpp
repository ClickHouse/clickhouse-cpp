#include <clickhouse/columns/itemview.h>
#include <clickhouse/columns/numeric.h>

#include <gtest/gtest.h>
#include "utils.h"

#include <string_view>
#include <limits>

namespace
{
using namespace clickhouse;
}

TEST(ItemView, StorableTypes) {
/// Validate that is it possible to store proper value of a proper type into a ItemView
/// and get it back with corresponding ItemView::get<T>

#define TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, NativeValue) \
    EXPECT_EQ(static_cast<NativeType>(NativeValue), ItemView(TypeCode, static_cast<NativeType>(NativeValue)).get<NativeType>()) \
        << " TypeCode:" << #TypeCode << " NativeType: " << #NativeType;

#define TEST_ITEMVIEW_TYPE_VALUES(TypeCode, NativeType) \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, std::numeric_limits<NativeType>::min()); \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, std::numeric_limits<NativeType>::min() + 1); \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, -1); \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, 0); \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, 1); \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, std::numeric_limits<NativeType>::max() - 1); \
    TEST_ITEMVIEW_TYPE_VALUE(TypeCode, NativeType, std::numeric_limits<NativeType>::max());

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Int8,  int8_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Int16, int16_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Int32, int32_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Int64, int64_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Int128, Int128);

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::UInt8,  uint8_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::UInt16, uint16_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::UInt32, uint32_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::UInt64, uint64_t);

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Float32, float);
    TEST_ITEMVIEW_TYPE_VALUE(Type::Code::Float32, float, 0.5);

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Float64, double);
    TEST_ITEMVIEW_TYPE_VALUE(Type::Code::Float64, double, 0.5);

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Date,       uint16_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::DateTime,   uint32_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::DateTime64, int64_t);

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal,    int32_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal,    int64_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal,    Int128);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal,    uint32_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal,    uint64_t);
//    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal,    UInt128);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal32,  int32_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal64,  int64_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Decimal128, Int128);

    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Enum8, uint8_t);
    TEST_ITEMVIEW_TYPE_VALUES(Type::Code::Enum16, uint16_t);

    TEST_ITEMVIEW_TYPE_VALUE(Type::Code::String, std::string_view, "");
    TEST_ITEMVIEW_TYPE_VALUE(Type::Code::String, std::string_view, "here is a string");

    TEST_ITEMVIEW_TYPE_VALUE(Type::Code::FixedString, std::string_view, "");
    TEST_ITEMVIEW_TYPE_VALUE(Type::Code::FixedString, std::string_view, "here is a string");
}

#define EXPECT_ITEMVIEW_ERROR(TypeCode, NativeType) \
    EXPECT_THROW(ItemView(TypeCode, static_cast<NativeType>(0)), AssertionError) \
        << " TypeCode:" << #TypeCode << " NativeType: " << #NativeType;

TEST(ItemView, ErrorTypes) {
    // Types that is impossible to store certain Type::Code into an ItemView.
    EXPECT_ITEMVIEW_ERROR(Type::Code::Array, int);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Nullable, int);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Tuple, int);
    EXPECT_ITEMVIEW_ERROR(Type::Code::LowCardinality, int);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Map, int);
}

TEST(ItemView, TypeSizeMismatch) {
    // Validate that it is impossible to initialize ItemView with mismatching Type::Code and native value.

    EXPECT_ITEMVIEW_ERROR(Type::Code::Int8,  int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int8,  int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int8,  int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int8,  Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Int16, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int16, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int16, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int16, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Int32, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int32, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int32, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int32, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Int64, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int64, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int64, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int64, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Int128, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int128, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int128, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Int128, int64_t);

    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt8,  int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt8,  int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt8,  int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt8,  Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt16, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt16, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt16, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt16, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt32, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt32, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt32, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt32, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt64, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt64, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt64, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::UInt64, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Float32, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float32, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float32, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float32, Int128);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float32, double);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Float64, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float64, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float64, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float64, Int128);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Float64, float);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Date, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Date, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Date, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Date, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime64, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime64, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime64, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::DateTime64, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal, int8_t);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal32, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal32, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal32, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal32, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal64, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal64, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal64, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal64, Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal128, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal128, int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal128, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Decimal128, int64_t);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum8,  int16_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum8,  int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum8,  int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum8,  Int128);

    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum16, int8_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum16, int32_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum16, int64_t);
    EXPECT_ITEMVIEW_ERROR(Type::Code::Enum16, Int128);
}

TEST(ItemView, Int128_values) {
    const auto vals = {
        std::numeric_limits<Int128>::min() + 2,
        std::numeric_limits<Int128>::min() + 1,
        std::numeric_limits<Int128>::min(),
        absl::MakeInt128(0xffffffffffffffffll - 2, 0),
        absl::MakeInt128(0xffffffffffffffffll - 1, 0),
        absl::MakeInt128(0xffffffffffffffffll, 0),
        absl::MakeInt128(0xffffffffffffffffll, 0xffffffffffffffffll),
        absl::MakeInt128(0, 0xffffffffffffffffll - 2),
        absl::MakeInt128(0, 0xffffffffffffffffll - 1),
        absl::MakeInt128(0, 0xffffffffffffffffll),
        Int128(-1),
        Int128(0),
        Int128(1),
        std::numeric_limits<Int128>::max() - 2,
        std::numeric_limits<Int128>::max() - 1,
        std::numeric_limits<Int128>::max(),
    };

    for (size_t i = 0; i < vals.size(); ++i)
    {
        const auto value = vals.begin()[i];
        const ItemView item_view(Type::Code::Decimal128, value);

        EXPECT_EQ(value, item_view.get<Int128>()) << "# index: " << i << " Int128 value: " << value;
    }
}
