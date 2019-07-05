#include <clickhouse/types/types.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

TEST(TypesCase, TypeName) {
    ASSERT_EQ(Type::CreateDate()->GetName(), "Date");

    ASSERT_EQ(Type::CreateArray(Type::CreateSimple<int32_t>())->GetName(), "Array(Int32)");

    ASSERT_EQ(Type::CreateNullable(Type::CreateSimple<int32_t>())->GetName(), "Nullable(Int32)");

    ASSERT_EQ(Type::CreateArray(Type::CreateSimple<int32_t>())->As<ArrayType>()->GetItemType()->GetCode(), Type::Int32);

    ASSERT_EQ(Type::CreateTuple({Type::CreateSimple<int32_t>(), Type::CreateString()})->GetName(), "Tuple(Int32, String)");
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

TEST(TypesCase, DecimalTypes) {
    // TODO: implement this test.
}
