#include <gtest/gtest.h>
#include "ut/value_generators.h"
#include "utils.h"

#include <initializer_list>
#include <limits>
#include <optional>
#include <vector>

TEST(CompareRecursive, CompareValues) {
    EXPECT_TRUE(CompareRecursive(1, 1));
    EXPECT_TRUE(CompareRecursive(1.0f, 1.0f));
    EXPECT_TRUE(CompareRecursive(1.0, 1.0));
    EXPECT_TRUE(CompareRecursive(1.0L, 1.0L));

    EXPECT_TRUE(CompareRecursive("1.0L", "1.0L"));
    EXPECT_TRUE(CompareRecursive(std::string{"1.0L"}, std::string{"1.0L"}));
    EXPECT_TRUE(CompareRecursive(std::string_view{"1.0L"}, std::string_view{"1.0L"}));
}

TEST(CompareRecursive, CompareContainers) {
    EXPECT_TRUE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 3}));
    EXPECT_TRUE(CompareRecursive(std::vector<int>{}, std::vector<int>{}));

    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 4}));
    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2}));

    // That would cause compile-time error:
    // EXPECT_FALSE(CompareRecursive(std::array{1, 2, 3}, 1));
}


TEST(CompareRecursive, CompareNestedContainers) {
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

TEST(CompareRecursive, Nan) {
    /// Even though NaN == NaN is FALSE, CompareRecursive must compare those as TRUE.

    const auto NaNf = std::numeric_limits<float>::quiet_NaN();
    const auto NaNd = std::numeric_limits<double>::quiet_NaN();

    EXPECT_TRUE(CompareRecursive(NaNf, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNd));

    EXPECT_TRUE(CompareRecursive(NaNf, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNf));

    // 1.0 is arbitrary here
    EXPECT_FALSE(CompareRecursive(NaNf, 1.0));
    EXPECT_FALSE(CompareRecursive(NaNf, 1.0));
    EXPECT_FALSE(CompareRecursive(1.0, NaNd));
    EXPECT_FALSE(CompareRecursive(1.0, NaNd));
}

TEST(CompareRecursive, Optional) {
    EXPECT_TRUE(CompareRecursive(1, std::optional{1}));
    EXPECT_TRUE(CompareRecursive(std::optional{1}, 1));
    EXPECT_TRUE(CompareRecursive(std::optional{1}, std::optional{1}));

    EXPECT_FALSE(CompareRecursive(2, std::optional{1}));
    EXPECT_FALSE(CompareRecursive(std::optional{1}, 2));
    EXPECT_FALSE(CompareRecursive(std::optional{2}, std::optional{1}));
    EXPECT_FALSE(CompareRecursive(std::optional{1}, std::optional{2}));
}

TEST(CompareRecursive, OptionalNan) {
    // Special case for optional comparison:
    // NaNs should be considered as equal (compare by unpacking value of optional)

    const auto NaNf = std::numeric_limits<float>::quiet_NaN();
    const auto NaNd = std::numeric_limits<double>::quiet_NaN();

    const auto NaNfo = std::optional{NaNf};
    const auto NaNdo = std::optional{NaNd};

    EXPECT_TRUE(CompareRecursive(NaNf, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNf, NaNfo));
    EXPECT_TRUE(CompareRecursive(NaNfo, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNfo, NaNfo));

    EXPECT_TRUE(CompareRecursive(NaNd, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNdo));
    EXPECT_TRUE(CompareRecursive(NaNdo, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNdo, NaNdo));

    EXPECT_FALSE(CompareRecursive(NaNdo, std::optional<double>{}));
    EXPECT_FALSE(CompareRecursive(NaNfo, std::optional<float>{}));
    EXPECT_FALSE(CompareRecursive(std::optional<double>{}, NaNdo));
    EXPECT_FALSE(CompareRecursive(std::optional<float>{}, NaNfo));

    // Too lazy to comparison code against std::nullopt, but this shouldn't be a problem in real life
    // following will produce compile time error:
//    EXPECT_FALSE(CompareRecursive(NaNdo, std::nullopt));
//    EXPECT_FALSE(CompareRecursive(NaNfo, std::nullopt));
}


TEST(Generators, MakeArrays) {
    auto arrays = MakeArrays<std::string, MakeStrings>();
    ASSERT_LT(0u, arrays.size());
}

class OutputTest : public ::testing::Test {
public:
    template <typename T>
    static std::string ToString(const T & t) {
        std::stringstream sstr;
        sstr << t;

        return sstr.str();
    }
};

TEST_F(OutputTest, PrettyPrintByteSize)
{
    EXPECT_EQ("3 bytes", ToString(PrettyPrintByteSize{3}));

    EXPECT_EQ("30 bytes", ToString(PrettyPrintByteSize{30}));
    EXPECT_EQ("300 bytes", ToString(PrettyPrintByteSize{300}));

    EXPECT_EQ("123 bytes", ToString(PrettyPrintByteSize{123}));

    for (const auto & [base, base_name] : std::initializer_list<std::pair<size_t, const char*>>{
            // {1,               "bytes"},
            {1024,            "KiB"},
            {1024*1024,       "MiB"},
            {1024*1024*1024,  "GiB"},
         } )
    {
        for (const auto & [value, value_str] : std::initializer_list<std::pair<float, const char*>>{
                {1, "1"},
                {1.01, "1.01"},
                {1.10, "1.1"},
                {1.5, "1.5"},
                {3, "3"},
                {3.25, "3.25"},
                {13.75, "13.75"},
                {135.5, "135.5"},
                {135.125, "135.125"},
                {10, "10"},
                {100, "100"},
                {1000, "1000"},
             })
        {
            const auto bytes_value = static_cast<size_t>(base * value);
            const auto expected_str = std::string(value_str) + " " + base_name;
            EXPECT_EQ(expected_str, ToString(PrettyPrintByteSize{bytes_value}))
                << "\n\tbase:      " << base
                << "\n\tbase_name: " << base_name
                << "\n\tvalue:     " << value
                << "\n\tvalue_str: " << value_str;
        }
    }
}
