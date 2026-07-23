#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/json.h>

#include <gtest/gtest.h>

namespace {
using namespace clickhouse;
}

TEST(CreateColumnByType, CreateSimpleAggregateFunction) {
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

TEST(CreateColumnByType, LowCardinalityAsWrappedColumn) {
    CreateColumnByTypeSettings create_column_settings;
    create_column_settings.low_cardinality_as_wrapped_column = true;

    ASSERT_EQ(Type::String, CreateColumnByType("LowCardinality(String)", create_column_settings)->GetType().GetCode());
    ASSERT_EQ(Type::String, CreateColumnByType("LowCardinality(String)", create_column_settings)->As<ColumnString>()->GetType().GetCode());

    ASSERT_EQ(Type::FixedString, CreateColumnByType("LowCardinality(FixedString(10000))", create_column_settings)->GetType().GetCode());
    ASSERT_EQ(Type::FixedString, CreateColumnByType("LowCardinality(FixedString(10000))", create_column_settings)->As<ColumnFixedString>()->GetType().GetCode());
}

TEST(CreateColumnByType, DateTime) {
    ASSERT_NE(nullptr, CreateColumnByType("DateTime"));
    ASSERT_NE(nullptr, CreateColumnByType("DateTime('Europe/Moscow')"));

    ASSERT_EQ(CreateColumnByType("DateTime('UTC')")->As<ColumnDateTime>()->Timezone(), "UTC");
    ASSERT_EQ(CreateColumnByType("DateTime64(3, 'UTC')")->As<ColumnDateTime64>()->Timezone(), "UTC");
    ASSERT_EQ(CreateColumnByType("DateTime('Etc/Can\\'t')")->As<ColumnDateTime>()->Timezone(), "Etc/Can't");
    ASSERT_EQ(CreateColumnByType("DateTime64(3, 'A\\\\B')")->As<ColumnDateTime64>()->Timezone(), "A\\B");
}

TEST(CreateColumnByType, EnumEscapedNames) {
    const std::vector<Type::EnumItem> enum_items = {{"can't", 1}, {"a\\b", 2}, {"a,b=(c)", 3}, {"", 4}};
    auto col                                     = CreateColumnByType("Enum8('can\\'t' = 1, 'a\\\\b' = 2, 'a,b=(c)' = 3, '' = 4)");
    ASSERT_NE(nullptr, col);
    ASSERT_TRUE(col->Type()->IsEqual(Type::CreateEnum8(enum_items)));
}

// Round-trip: build a Type from raw values, render it via GetName() (escape),
// re-parse the rendered name via CreateColumnByType (unescape) and verify the
// raw values survive the render->parse cycle. Uses only escape symbols that are
// currently implemented.

TEST(CreateColumnByType, RoundTrip_Enum) {
    const std::vector<Type::EnumItem> enum_items = {{"can't", 1}, {"a\\b", 2}, {"tab\there", 3}, {"line\nbreak", 4}};
    auto type = Type::CreateEnum8(enum_items);

    auto col = CreateColumnByType(type->GetName());
    ASSERT_NE(nullptr, col);
    EXPECT_TRUE(col->Type()->IsEqual(type));

    const auto* enum_type = col->Type()->As<EnumType>();
    ASSERT_NE(nullptr, enum_type);
    EXPECT_EQ(enum_type->GetEnumName(1), "can't");
    EXPECT_EQ(enum_type->GetEnumValue("a\\b"), 2);
    EXPECT_EQ(enum_type->GetEnumName(3), "tab\there");
    EXPECT_EQ(enum_type->GetEnumValue("line\nbreak"), 4);
}

TEST(CreateColumnByType, RoundTrip_DateTime) {
    auto type = Type::CreateDateTime("Etc/Can't");

    auto col = CreateColumnByType(type->GetName());
    ASSERT_NE(nullptr, col);
    EXPECT_EQ(col->As<ColumnDateTime>()->Timezone(), "Etc/Can't");
}

TEST(CreateColumnByType, RoundTrip_DateTime64) {
    auto type = Type::CreateDateTime64(3, "A\\B");

    auto col = CreateColumnByType(type->GetName());
    ASSERT_NE(nullptr, col);
    EXPECT_EQ(col->As<ColumnDateTime64>()->Timezone(), "A\\B");
}

TEST(CreateColumnByType, RoundTrip_Tuple) {
    const std::vector<std::string> item_names = {"a`b", "c.d"};
    auto type = Type::CreateTuple({Type::CreateSimple<uint8_t>(), Type::CreateString()}, item_names);

    auto col = CreateColumnByType(type->GetName());
    ASSERT_NE(nullptr, col);

    const auto* tuple_type = col->Type()->As<TupleType>();
    ASSERT_NE(nullptr, tuple_type);
    EXPECT_EQ(tuple_type->GetItemNames(), item_names);
}

TEST(CreateColumnByType, AggregateFunction) {
    EXPECT_EQ(nullptr, CreateColumnByType("AggregateFunction(argMax, Int32, DateTime64(3))"));
    EXPECT_EQ(nullptr, CreateColumnByType("AggregateFunction(argMax, FIxedString(10), DateTime64(3, 'UTC'))"));
}


class CreateColumnByTypeWithName : public ::testing::TestWithParam<const char* /*Column Type String*/>
{};

TEST(CreateColumnByType, Bool) {
    const auto col = CreateColumnByType("Bool");
    ASSERT_NE(nullptr, col);
#if CH_MAP_BOOL_TO_UINT8
    EXPECT_EQ(col->GetType().GetName(), "UInt8");
    EXPECT_EQ(col->GetType().GetCode(), Type::UInt8);
    EXPECT_NE(nullptr, col->As<ColumnUInt8>());
#else
    EXPECT_EQ(col->GetType().GetName(), "Bool");
    EXPECT_EQ(col->GetType().GetCode(), Type::Bool);
    EXPECT_NE(nullptr, col->As<ColumnBool>());
#endif
}

TEST_P(CreateColumnByTypeWithName, CreateColumnByType)
{
    const auto col = CreateColumnByType(GetParam());
    ASSERT_NE(nullptr, col);
    EXPECT_EQ(col->GetType().GetName(), GetParam());
}

INSTANTIATE_TEST_SUITE_P(Basic, CreateColumnByTypeWithName, ::testing::Values(
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "String", "Date", "DateTime",
    "UUID", "Int128", "UInt128"
));
#if !CH_MAP_BOOL_TO_UINT8
INSTANTIATE_TEST_SUITE_P(BasicBool, CreateColumnByTypeWithName, ::testing::Values("Bool"));
#endif

INSTANTIATE_TEST_SUITE_P(Parametrized, CreateColumnByTypeWithName, ::testing::Values(
    "FixedString(0)", "FixedString(10000)",
    "DateTime('UTC')", "DateTime64(3, 'UTC')",
    "Decimal(9,3)", "Decimal(18,3)",
    "Enum8('ONE' = 1, 'TWO' = 2)",
    "Enum16('ONE' = 1, 'TWO' = 2, 'THREE' = 3, 'FOUR' = 4)"
));


INSTANTIATE_TEST_SUITE_P(Nested, CreateColumnByTypeWithName, ::testing::Values(
    "Nullable(FixedString(10000))",
    "Nullable(LowCardinality(FixedString(10000)))",
    "Array(Nullable(LowCardinality(FixedString(10000))))",
    "Array(Enum8('ONE' = 1, 'TWO' = 2))"
));
