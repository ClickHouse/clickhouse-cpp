#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

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
}

class CreateColumnByTypeWithName : public ::testing::TestWithParam<const char* /*Column Type String*/>
{};

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
    "UUID", "Int128"
));

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
