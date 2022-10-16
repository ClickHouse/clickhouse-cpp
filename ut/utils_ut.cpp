#include <gtest/gtest.h>
#include "utils.h"

#include <vector>

TEST(TestCompareContainer, ComparePlain) {
    EXPECT_TRUE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 3}));
    EXPECT_TRUE(CompareRecursive(std::vector<int>{}, std::vector<int>{}));

    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 4}));
    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2}));

    // That would cause compile-time error:
    // EXPECT_FALSE(CompareRecursive(std::array{1, 2, 3}, 1));
}


TEST(TestCompareContainer, CompareNested) {
    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}}},
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}}}));

    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}, {}}},
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}, {}}}));

    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{}}},
        std::vector<std::vector<int>>{{{}}}));

    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 7}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {4, 5}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{}}));
}

TEST(StringUtils, UUID) {
    const clickhouse::UUID& uuid{0x0102030405060708, 0x090a0b0c0d0e0f10};
    const std::string uuid_string = "01020304-0506-0708-090a-0b0c0d0e0f10";
    EXPECT_EQ(ToString(uuid), uuid_string);
}
