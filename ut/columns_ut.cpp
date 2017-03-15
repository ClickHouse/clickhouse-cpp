#include <clickhouse/columns/numeric.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

static const std::vector<uint32_t> kNumbers =
    {1, 2, 3, 7, 11, 13, 17, 19, 23, 29, 31};


TEST(ColumnsCase, NumericInit) {
    auto col = std::make_shared<ColumnUInt32>(kNumbers);

    ASSERT_EQ(col->Size(), 11u);
    ASSERT_EQ(col->At(3),   7u);
    ASSERT_EQ(col->At(10), 31u);

    auto sun = std::make_shared<ColumnUInt32>(kNumbers);
}

TEST(ColumnsCase, NumericSlice) {
    auto col = std::make_shared<ColumnUInt32>(kNumbers);
    auto sub = col->Slice(3, 3)->As<ColumnUInt32>();

    ASSERT_EQ(sub->Size(), 3u);
    ASSERT_EQ(sub->At(0),  7u);
    ASSERT_EQ(sub->At(2), 13u);
}
