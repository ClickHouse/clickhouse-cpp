#include <clickhouse/types/types.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

TEST(TypesCase, TypeName) {
    ASSERT_EQ(
        Type::CreateDate()->GetName(),
        "Date"
    );

    ASSERT_EQ(
        Type::CreateArray(Type::CreateSimple<int32_t>())->GetName(),
        "Array(Int32)"
    );

    ASSERT_EQ(
        Type::CreateNullable(Type::CreateSimple<int32_t>())->GetName(),
        "Nullable(Int32)"
    );

    ASSERT_EQ(
        Type::CreateArray(Type::CreateSimple<int32_t>())->GetItemType()->GetCode(),
        Type::Int32
    );

    ASSERT_EQ(
        Type::CreateTuple({
            Type::CreateSimple<int32_t>(),
            Type::CreateString()})->GetName(),
        "Tuple(Int32, String)"
    );
}

TEST(TypesCase, NullableType) {
    TypeRef nested = Type::CreateSimple<int32_t>();
    ASSERT_EQ(
        Type::CreateNullable(nested)->GetNestedType(),
        nested
    );
}

TEST(TypesCase, EnumTypes) {
    EnumType enum8(Type::CreateEnum8({{"One", 1}, {"Two", 2}}));
    ASSERT_EQ(enum8.GetName(), "Enum8('One' = 1, 'Two' = 2)");
    ASSERT_TRUE(enum8.HasEnumValue(1));
    ASSERT_TRUE(enum8.HasEnumName("Two"));
    ASSERT_FALSE(enum8.HasEnumValue(10));
    ASSERT_FALSE(enum8.HasEnumName("Ten"));
    ASSERT_EQ(enum8.GetEnumName(2), "Two");
    ASSERT_EQ(enum8.GetEnumValue("Two"), 2);

    EnumType enum16(Type::CreateEnum16({{"Green", 1}, {"Red", 2}, {"Yellow", 3}}));
    ASSERT_EQ(enum16.GetName(), "Enum16('Green' = 1, 'Red' = 2, 'Yellow' = 3)");
    ASSERT_TRUE(enum16.HasEnumValue(3));
    ASSERT_TRUE(enum16.HasEnumName("Green"));
    ASSERT_FALSE(enum16.HasEnumValue(10));
    ASSERT_FALSE(enum16.HasEnumName("Black"));
    ASSERT_EQ(enum16.GetEnumName(2), "Red");
    ASSERT_EQ(enum16.GetEnumValue("Green"), 1);

    ASSERT_EQ(std::distance(enum16.BeginValueToName(), enum16.EndValueToName()), 3u);
    ASSERT_EQ((*enum16.BeginValueToName()).first, 1);
    ASSERT_EQ((*enum16.BeginValueToName()).second, "Green");
    ASSERT_EQ((*(++enum16.BeginValueToName())).first, 2);
    ASSERT_EQ((*(++enum16.BeginValueToName())).second, "Red");
}
