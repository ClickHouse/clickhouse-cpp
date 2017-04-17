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
