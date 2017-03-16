#include <clickhouse/columns/date.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <contrib/gtest/gtest.h>

using namespace clickhouse;

static std::vector<uint32_t> MakeNumbers() {
    return std::vector<uint32_t>
        {1, 2, 3, 7, 11, 13, 17, 19, 23, 29, 31};
}

static std::vector<std::string> MakeFixedStrings() {
    return std::vector<std::string>
        {"aaa", "bbb", "ccc", "ddd"};
}

static std::vector<std::string> MakeStrings() {
    return std::vector<std::string>
        {"a", "ab", "abc", "abcd"};
}


TEST(ColumnsCase, NumericInit) {
    auto col = std::make_shared<ColumnUInt32>(MakeNumbers());

    ASSERT_EQ(col->Size(), 11u);
    ASSERT_EQ(col->At(3),   7u);
    ASSERT_EQ(col->At(10), 31u);

    auto sun = std::make_shared<ColumnUInt32>(MakeNumbers());
}

TEST(ColumnsCase, NumericSlice) {
    auto col = std::make_shared<ColumnUInt32>(MakeNumbers());
    auto sub = col->Slice(3, 3)->As<ColumnUInt32>();

    ASSERT_EQ(sub->Size(), 3u);
    ASSERT_EQ(sub->At(0),  7u);
    ASSERT_EQ(sub->At(2), 13u);
}


TEST(ColumnsCase, FixedStringInit) {
    auto col = std::make_shared<ColumnFixedString>(3);
    for (const auto& s : MakeFixedStrings()) {
        col->Append(s);
    }

    ASSERT_EQ(col->Size(), 4u);
    ASSERT_EQ(col->At(1), "bbb");
    ASSERT_EQ(col->At(3), "ddd");
}

TEST(ColumnsCase, StringInit) {
    auto col = std::make_shared<ColumnString>(MakeStrings());

    ASSERT_EQ(col->Size(), 4u);
    ASSERT_EQ(col->At(1), "ab");
    ASSERT_EQ(col->At(3), "abcd");
}


TEST(ColumnsCase, DateAppend) {
    auto col1 = std::make_shared<ColumnDate>();
    auto col2 = std::make_shared<ColumnDate>();
    auto now  = std::time(nullptr);

    col1->Append(now);
    col2->Append(col1);

    ASSERT_EQ(col2->Size(), 1u);
    ASSERT_EQ(col2->At(0), (now / 86400) * 86400);
}
