#include <clickhouse/types/types.h>
#include <clickhouse/columns/factory.h>
#include <ut/utils.h>

#include <gtest/gtest.h>

using namespace clickhouse;

TEST(TypesCase, TypeName) {
    ASSERT_EQ(Type::CreateDate()->GetName(), "Date");

    ASSERT_EQ(Type::CreateArray(Type::CreateSimple<int32_t>())->GetName(), "Array(Int32)");

    ASSERT_EQ(Type::CreateNullable(Type::CreateSimple<int32_t>())->GetName(), "Nullable(Int32)");

    ASSERT_EQ(Type::CreateArray(Type::CreateSimple<int32_t>())->As<ArrayType>()->GetItemType()->GetCode(), Type::Int32);

    ASSERT_EQ(Type::CreateTuple({Type::CreateSimple<int32_t>(), Type::CreateString()})->GetName(), "Tuple(Int32, String)");

    ASSERT_EQ(
        Type::CreateTuple({
            Type::CreateSimple<int32_t>(),
            Type::CreateString()})->GetName(),
        "Tuple(Int32, String)"
    );

    ASSERT_EQ(
        Type::CreateEnum8({{"One", 1}})->GetName(),
        "Enum8('One' = 1)"
    );
    ASSERT_EQ(
        Type::CreateEnum8({})->GetName(),
        "Enum8()"
    );
}

TEST(TypesCase, NullableType) {
    TypeRef nested = Type::CreateSimple<int32_t>();
    ASSERT_EQ(Type::CreateNullable(nested)->As<NullableType>()->GetNestedType(), nested);
}

TEST(TypesCase, EnumTypes) {
    auto enum8 = Type::CreateEnum8({{"One", 1}, {"Two", 2}});
    ASSERT_EQ(enum8->GetName(), "Enum8('One' = 1, 'Two' = 2)");
    ASSERT_TRUE(enum8->As<EnumType>()->HasEnumValue(1));
    ASSERT_TRUE(enum8->As<EnumType>()->HasEnumName("Two"));
    ASSERT_FALSE(enum8->As<EnumType>()->HasEnumValue(10));
    ASSERT_FALSE(enum8->As<EnumType>()->HasEnumName("Ten"));
    ASSERT_EQ(enum8->As<EnumType>()->GetEnumName(2), "Two");
    ASSERT_EQ(enum8->As<EnumType>()->GetEnumValue("Two"), 2);

    auto enum16 = Type::CreateEnum16({{"Green", 1}, {"Red", 2}, {"Yellow", 3}});
    ASSERT_EQ(enum16->GetName(), "Enum16('Green' = 1, 'Red' = 2, 'Yellow' = 3)");
    ASSERT_TRUE(enum16->As<EnumType>()->HasEnumValue(3));
    ASSERT_TRUE(enum16->As<EnumType>()->HasEnumName("Green"));
    ASSERT_FALSE(enum16->As<EnumType>()->HasEnumValue(10));
    ASSERT_FALSE(enum16->As<EnumType>()->HasEnumName("Black"));
    ASSERT_EQ(enum16->As<EnumType>()->GetEnumName(2), "Red");
    ASSERT_EQ(enum16->As<EnumType>()->GetEnumValue("Green"), 1);

    ASSERT_EQ(std::distance(enum16->As<EnumType>()->BeginValueToName(), enum16->As<EnumType>()->EndValueToName()), 3u);
    ASSERT_EQ(enum16->As<EnumType>()->BeginValueToName()->first, 1);
    ASSERT_EQ(enum16->As<EnumType>()->BeginValueToName()->second, "Green");
    ASSERT_EQ((++enum16->As<EnumType>()->BeginValueToName())->first, 2);
    ASSERT_EQ((++enum16->As<EnumType>()->BeginValueToName())->second, "Red");
}

TEST(TypesCase, EnumTypesEmpty) {
    ASSERT_EQ("Enum8()", Type::CreateEnum8({})->GetName());
    ASSERT_EQ("Enum16()", Type::CreateEnum16({})->GetName());
}

TEST(TypesCase, DecimalTypes) {
    // TODO: implement this test.
}

TEST(TypesCase, IsEqual) {
    const std::string type_names[] = {
        "UInt8",
        "Int8",
        "UInt128",
        "String",
        "FixedString(0)",
        "FixedString(10000)",
        "DateTime('UTC')",
        "DateTime64(3, 'UTC')",
        "Decimal(9,3)",
        "Decimal(18,3)",
        "Enum8('ONE' = 1)",
        "Enum8('ONE' = 1, 'TWO' = 2)",
        "Enum16('ONE' = 1, 'TWO' = 2, 'THREE' = 3, 'FOUR' = 4)",
        "Nullable(FixedString(10000))",
        "Nullable(LowCardinality(FixedString(10000)))",
        "Array(Int8)",
        "Array(UInt8)",
        "Array(String)",
        "Array(Nullable(LowCardinality(FixedString(10000))))",
        "Array(Enum8('ONE' = 1, 'TWO' = 2))"
        "Tuple(String, Int8, Date, DateTime)",
        "Nullable(Tuple(String, Int8, Date, DateTime))",
        "Array(Nullable(Tuple(String, Int8, Date, DateTime)))",
        "Array(Array(Nullable(Tuple(String, Int8, Date, DateTime))))",
        "Array(Array(Array(Nullable(Tuple(String, Int8, Date, DateTime)))))",
        "Array(Array(Array(Array(Nullable(Tuple(String, Int8, Date, DateTime('UTC')))))))"
        "Array(Array(Array(Array(Nullable(Tuple(String, Int8, Date, DateTime('UTC'), Tuple(LowCardinality(String), Enum8('READ'=1, 'WRITE'=0))))))))",
    };

    // Check that Type::IsEqual returns true only if:
    // - same Type instance
    // - same Type layout (matching outer type with all nested types and/or parameters)
    for (const auto & type_name : type_names) {
        SCOPED_TRACE(type_name);
        const auto type = clickhouse::CreateColumnByType(type_name)->Type();

        // Should be equal to itself
        EXPECT_TRUE(type->IsEqual(type));
        EXPECT_TRUE(type->IsEqual(*type));

        for (const auto & other_type_name : type_names) {
            const auto other_type = clickhouse::CreateColumnByType(other_type_name)->Type();

            const auto should_be_equal = type_name == other_type_name;
            EXPECT_EQ(should_be_equal, type->IsEqual(other_type))
                        << "For types: " << type_name << " and " << other_type_name;
        }
    }
}

TEST(TypesCase, ErrorEnumContent) {
    const std::string type_names[] = {
        "Enum8()",
        "Enum8('ONE')",
        "Enum8('ONE'=1,'TWO')",
        "Enum16('TWO'=,'TWO')",
    };
 
    for (const auto& type_name : type_names) {
        SCOPED_TRACE(type_name);
        EXPECT_THROW(clickhouse::CreateColumnByType(type_name)->Type(), ValidationError);
    }
}