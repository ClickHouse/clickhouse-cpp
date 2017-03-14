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
        Type::CreateArray(Type::CreateSimple<int32_t>())->GetItemType()->GetCode(),
        Type::Int32
    );
}
