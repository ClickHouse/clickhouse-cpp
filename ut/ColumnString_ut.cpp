#include <clickhouse/columns/string.h>

#include <gtest/gtest.h>
#include "clickhouse/exceptions.h"
#include "gtest/gtest-message.h"
#include "gtest/gtest-param-test.h"
#include "ut/utils_comparison.h"
#include "utils.h"
#include "value_generators.h"

#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <ios>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string_view>
#include <sstream>
#include <vector>
#include <random>

using namespace clickhouse;

namespace
{
// TODO: convert to generator with container-like interface (for comparison with CompareRecursive), should implement size(), begin() and end(), iterator i with ++i and *i
size_t EstimateColumnStringMemoryUsage(
        size_t number_of_items,                               // number of items in column
        ColumnString::EstimatedValueSize item_estimated_size, // estimated item size
        float value_to_estimation_average_size_ratio = 1.0,   // expected real item size to estimated item size
        std::optional<size_t> total_items_size = std::nullopt  // total length of all items
    ) {
    static const size_t COLUMN_STRING_DEFAULT_BLOCK_SIZE = 4096;
    static const size_t COLUMN_STRING_MAX_EXPECTED_MEMORY_OVERHEAD = 4096;

    // if no estimation provided, use factual total size of all items
    if (item_estimated_size == ColumnString::EstimatedValueSize{0} && total_items_size && number_of_items)
        item_estimated_size = ColumnString::EstimatedValueSize(static_cast<double>(*total_items_size) / number_of_items);

    const size_t estimated_total_item_size = number_of_items * static_cast<size_t>(item_estimated_size) * value_to_estimation_average_size_ratio;
    const auto estimated_number_of_blocks = std::max<size_t>(1, estimated_total_item_size ? COLUMN_STRING_DEFAULT_BLOCK_SIZE / estimated_total_item_size : 1);

    // space wasted in block since not all items can be fit perfectly, and there is some unused space at the end of the block.
    const auto estimate_lost_space_in_block = (static_cast<size_t>(item_estimated_size) != 0
            ? COLUMN_STRING_DEFAULT_BLOCK_SIZE % static_cast<size_t>(static_cast<size_t>(item_estimated_size) * value_to_estimation_average_size_ratio)
            : COLUMN_STRING_DEFAULT_BLOCK_SIZE / 10);

    const auto max_estimation_error_factor = item_estimated_size == ColumnString::NO_PREALLOCATE ? 2.5 : 2;

    return (number_of_items * sizeof(std::string_view)
        + estimated_total_item_size
        + estimate_lost_space_in_block * estimated_number_of_blocks
        + COLUMN_STRING_DEFAULT_BLOCK_SIZE
        // It is hard to compute overhead added by vector<ColumnString::Block>
        // (mostly because we don't know number of ColumnString::Block instances from outside, and this number depends on many factors),
        // so we just make a guess.
            + COLUMN_STRING_MAX_EXPECTED_MEMORY_OVERHEAD)
        * max_estimation_error_factor;
}

std::string ScaleString(std::string str, size_t required_size) {
    if (required_size < str.length()) {
        str.resize(required_size);

        return str;
    }

    str.reserve(required_size);
    while (str.length() < required_size) {
        const size_t remaining_size = required_size - str.length();
        str.insert(str.length(), str.data(), std::min(str.length(), remaining_size));
    }
    str.resize(required_size);

    return str;
}

std::vector<std::string> GenerateSizedStrings(
        const std::vector<std::string>& initial_values, // values from which result is generated
        size_t required_number_of_items,                // number of strings in result
        size_t required_single_value_size,              // how long should resulting strings be
        const std::vector<float>& scale_factors = {1.0} // length variations on resulting string, must be > 0
    ) {
    std::vector<std::string> result;
    result.reserve(required_number_of_items);

    for (size_t i = 0; i < required_number_of_items; ++i) {
        const auto & value = initial_values[i % initial_values.size()];
        const auto & scale_factor = scale_factors[i % scale_factors.size()];

        size_t value_size = required_single_value_size;
        if (value_size == 0)
            value_size = value.length();
        value_size = std::lround(value_size * scale_factor);

        result.push_back(ScaleString(value, value_size));
    }

    return result;
}

// class GenerateSizedStrings
// {
//     const std::vector<std::string>& initial_values;  // values from which result is generated
//     size_t required_single_value_size;               // how long should resulting strings be
//     const std::vector<float>& scale_factors = {1.0}; // length variations on resulting string, must be > 0

// public:
//     GenerateSizedStrings(
//         const std::vector<std::string>& initial_values, // values from which result is generated
//         // size_t required_number_of_items,                // number of strings in result
//         size_t required_single_value_size,              // how long should resulting strings be
//         const std::vector<float>& scale_factors = {1.0} // length variations on resulting string, must be > 0
//     )
//         : initial_values(initial_values)
//         , required_single_value_size(required_single_value_size)
//         , scale_factors(scale_factors)
//     {
//     }

//     std::string operator()(size_t i) const {
//         const auto & value = initial_values[i % initial_values.size()];
//         const auto & scale_factor = scale_factors[i % scale_factors.size()];

//         size_t value_size = required_single_value_size;
//         if (value_size == 0)
//             value_size = value.length();
//         value_size = std::lround(value_size * scale_factor);

//         return ScaleString(value, value_size);
//     }
// };




}

TEST(ColumnString, ConstructorThatCopiesValues) {
    auto values = MakeStrings();
    auto col = std::make_shared<ColumnString>(values);

    ASSERT_EQ(col->Size(), values.size());
    ASSERT_EQ(col->At(1), "ab");
    ASSERT_EQ(col->At(3), "abcd");
}

TEST(ColumnString, ConstructorThatMovesValues) {
    auto values = MakeStrings();
    auto copy = values;
    auto col = ColumnString(std::move(copy));

    EXPECT_TRUE(CompareRecursive(values, col));
}

TEST(ColumnString, Append) {
    auto col = std::make_shared<ColumnString>();
    const char* expected = "ufiudhf3493fyiudferyer3yrifhdflkdjfeuroe";
    std::string data(expected);
    col->Append(data);
    col->Append(std::move(data));
    col->Append("11");

    ASSERT_EQ(col->Size(), 3u);
    ASSERT_EQ(col->At(0), expected);
    ASSERT_EQ(col->At(1), expected);
    ASSERT_EQ(col->At(2), "11");
}

//TEST(ColumnString, DefaultSizeEstimation) {
//    auto values = MakeStrings();

//    const ColumnString::EstimatedValueSize value_size_estimations[] = {
//        ColumnString::EstimatedValueSize::TINY,
//        ColumnString::EstimatedValueSize::SMALL,
//        ColumnString::EstimatedValueSize::MEDIUM,
//        ColumnString::EstimatedValueSize::LARGE,
//        ColumnString::EstimatedValueSize::HUGE,
//    };

//    for (auto estimation : value_size_estimations) {
//        SCOPED_TRACE(::testing::Message("with estimation: ") << estimation);

//        auto col = std::make_shared<ColumnString>(estimation);

//        col->Reserve(values.size());

//        size_t i = 0;
//        for (const auto & v : values) {
//            col->Append(v);

//            EXPECT_EQ(i + 1, col->Size());
//            EXPECT_EQ(v, col->At(i));

//            ++i;
//        }
//    }
//}

TEST(ColumnString, InvalidSizeEstimation) {
    // Negative values or values that are too big (> INTMAX) that are wrapped and implicitly converted to negative
    // should cause an exception.

    EXPECT_THROW(std::make_shared<ColumnString>(ColumnString::EstimatedValueSize(-1)), ValidationError);
    EXPECT_THROW(std::make_shared<ColumnString>(ColumnString::EstimatedValueSize(static_cast<size_t>(std::numeric_limits<int>::max()) + 1)), ValidationError);
    EXPECT_THROW(std::make_shared<ColumnString>(ColumnString::EstimatedValueSize(std::numeric_limits<size_t>::max())), ValidationError);

    ColumnString col;
    EXPECT_THROW(col.SetEstimatedValueSize(ColumnString::EstimatedValueSize(-1)), ValidationError);
    EXPECT_THROW(col.SetEstimatedValueSize(ColumnString::EstimatedValueSize(static_cast<size_t>(std::numeric_limits<int>::max()) + 1)), ValidationError);
    EXPECT_THROW(col.SetEstimatedValueSize(ColumnString::EstimatedValueSize(std::numeric_limits<size_t>::max())), ValidationError);
}

TEST(ColumnString, WithSizeEstimation) {
    const ColumnString::EstimatedValueSize value_size_estimations[] = {
        ColumnString::EstimatedValueSize::TINY,
        ColumnString::EstimatedValueSize::SMALL,
        ColumnString::EstimatedValueSize::MEDIUM,
        ColumnString::EstimatedValueSize::LARGE,

        //        ColumnString::EstimatedValueSize(0),
        ColumnString::EstimatedValueSize(1),
        ColumnString::EstimatedValueSize(300),
        ColumnString::EstimatedValueSize(10'000),
    };

    auto values = MakeStrings();
    std::cerr << "Number of values: " << values.size() << std::endl;

    for (ColumnString::EstimatedValueSize estimation : value_size_estimations) {
        SCOPED_TRACE(::testing::Message("with estimation: ") << estimation);
        std::cerr << "\nEstimation " << estimation << std::endl;

        auto col = std::make_shared<ColumnString>(estimation);

        dumpMemoryUsage("After constructing with estimation", col);

        col->Reserve(values.size());
        dumpMemoryUsage("After Reserve()", col);

        size_t i = 0;
        for (const auto & v : values) {
            col->Append(v);

            EXPECT_EQ(i + 1, col->Size());
            EXPECT_EQ(v, col->At(i));

            ++i;
        }

        dumpMemoryUsage("After appending all values", col);
    }
}



struct SizeRatio {
    std::vector<float> ratios;
    float average;

    SizeRatio(std::vector<float> ratios_)
        : ratios(std::move(ratios_))
    {
        float sum = 0;
        for (const auto & r : ratios) {
            sum += r;
        }

        average = sum / ratios.size();
    }
};

std::ostream & operator<<(std::ostream& ostr, const SizeRatio & r) {
    return ostr << "SizeRatio{ average: " << std::fixed << r.average << " } ";
}

//std::vector<SizeRatio> ratios{
//                              // estimation is about right
//                              SizeRatio({0.9, 0.95, 1.0, 1.05, 1.1}),
//                              // estimation is a bit high, real values are about 0.8 of estimated size
//                              SizeRatio({0.75, 0.8, 0.85}),
//                              // estimation is a bit low, real values are about 1.2 of estimated size
//                              SizeRatio({1.25, 1.2, 1.25}),
//                              // estimation is to high, real values are about 2.0 of estimated size
//                              SizeRatio({1.9, 2, 2.1}),
//                              // estimation is to low, real values are about 0.5 of estimated size
//                              SizeRatio({0.4, 0.5, 0.6}),
//                              };

/** Make sure that setting value size estimates with ColumnString::EstimatedValueSize either via contructor or via SetEstimatedValueSize
 *  doesn't break ColumnString functionality and well-behaves with Reserve() and Append().
 *  I.e. values are appended properly, nothing crashes and memory usage is not crazy-high if estimation is incorrect.
 */
struct ColumnStringEstimatedValueSizeTest : public ::testing::TestWithParam<std::tuple<ColumnString::EstimatedValueSize, SizeRatio>>
{
    void SetUp() override {
        const size_t MAX_MEMORY_USAGE = 100 * 1024 * 1024;
        const auto & [single_value_size_estimation, size_ratio] = GetParam();

        // Adjust number of items so the test doesn't use too much memory
        if (static_cast<size_t>(single_value_size_estimation) != 0
            // *2 since we store both reference values and values in column itself.
                && EstimateColumnStringMemoryUsage(expected_number_of_items, single_value_size_estimation, size_ratio.average) > MAX_MEMORY_USAGE) {
            const auto old_expected_number_of_items = expected_number_of_items;
            expected_number_of_items = MAX_MEMORY_USAGE / (static_cast<size_t>(single_value_size_estimation) * 2 * size_ratio.average);

            std::cerr << "To avoid using too much memory, reduced number of items in test"
                      << " from " << old_expected_number_of_items
                      << " to " << expected_number_of_items
                      << ", expected item size is " << single_value_size_estimation
                      << std::endl;
        }
    }

    size_t expected_number_of_items = 10000;

    void AppendStrings(
            ColumnString & column,
            size_t & total_values_size) const {

        const auto & [single_value_size_estimation, size_ratio] = GetParam();

        const auto values = GenerateSizedStrings(
            MakeStrings(),
            expected_number_of_items,
            static_cast<size_t>(single_value_size_estimation),
            size_ratio.ratios);

        total_values_size = 0;
        for (const auto & v : values) {
            total_values_size += v.size();

            column.Append(v);
        }

        ASSERT_TRUE(CompareRecursive(values, column));
    }

    size_t EstimateMemoryUsage(size_t total_values_size, float expected_number_of_items_multiplier = 1.0) {
        const auto & [single_value_size_estimation, size_ratio] = GetParam();
        return EstimateColumnStringMemoryUsage(expected_number_of_items * expected_number_of_items_multiplier, single_value_size_estimation, size_ratio.average, total_values_size);
    }
};

TEST_P(ColumnStringEstimatedValueSizeTest, ConstructorWithEstimation) {
    const auto & [single_value_size_estimation, size_ratio] = GetParam();

    ColumnString col(single_value_size_estimation);

    // basically no memory pre-allocated
    EXPECT_LT(col.MemoryUsage(), 1u);
}

//TEST_P(ColumnStringEstimatedValueSizeTest, ConstructorWithEstimationAsInt) {
////    auto single_value_size_estimation = GetParam();
//    ColumnString col(1);

//    // basically no memory pre-allocated except for some constant factor
//    EXPECT_LT(col.MemoryUsage(), 1u);
//}

TEST_P(ColumnStringEstimatedValueSizeTest, ConstructorWithNumberOfItemsAndEstimation) {
    // Constructor that receives both number of items and estimation pre-allocates memory for given number of items of estimated size.
    const auto & [single_value_size_estimation, size_ratio] = GetParam();

    ColumnString col(expected_number_of_items, single_value_size_estimation);

    // space for at least all the items, maybe more
    EXPECT_GT(col.MemoryUsage(), static_cast<size_t>(single_value_size_estimation) * expected_number_of_items);

    // but not too much
    EXPECT_LT(col.MemoryUsage(), EstimateColumnStringMemoryUsage(expected_number_of_items, single_value_size_estimation));
}

TEST_P(ColumnStringEstimatedValueSizeTest, AppendNoReserve)
{
    const auto & [single_value_size_estimation, size_ratio] = GetParam();

    auto col = ColumnString(single_value_size_estimation);
    size_t total_values_size = 0;

    EXPECT_NO_FATAL_FAILURE(AppendStrings(col, total_values_size));

    const auto max_estimation_error_factor = single_value_size_estimation == ColumnString::NO_PREALLOCATE ? 2.5 : 2;

    // since there was no Reserve call prior, there could be more some overallocations, hence some estimation error
    EXPECT_LT(col.MemoryUsage(), EstimateMemoryUsage(total_values_size) * max_estimation_error_factor);
}

TEST_P(ColumnStringEstimatedValueSizeTest, ReserveExactAndAppend)
{
    const auto & [single_value_size_estimation, size_ratio] = GetParam();

    auto col = ColumnString(single_value_size_estimation);
    size_t total_values_size = 0;

    EXPECT_NO_THROW(col.Reserve(expected_number_of_items));
    EXPECT_NO_FATAL_FAILURE(AppendStrings(col, total_values_size));

    const auto max_estimation_error_factor = single_value_size_estimation == ColumnString::NO_PREALLOCATE ? 2.5 : 2;

    // Allow minor overallocations, hence * 1.2
    EXPECT_LT(col.MemoryUsage(), EstimateMemoryUsage(total_values_size) * max_estimation_error_factor);
}

TEST_P(ColumnStringEstimatedValueSizeTest, ReserveLessAndAppend)
{
    const auto & [single_value_size_estimation, size_ratio] = GetParam();

    auto col = ColumnString(single_value_size_estimation);
    size_t total_values_size = 0;

    EXPECT_NO_THROW(col.Reserve(expected_number_of_items * .8));
    EXPECT_NO_FATAL_FAILURE(AppendStrings(col, total_values_size));

    const auto max_estimation_error_factor = single_value_size_estimation == ColumnString::NO_PREALLOCATE ? 2.5 : 2;

    // Allow minor overallocations, hence * 1.2
    EXPECT_LT(col.MemoryUsage(), EstimateMemoryUsage(total_values_size) * max_estimation_error_factor);
}

TEST_P(ColumnStringEstimatedValueSizeTest, ReserveMoreAndAppend)
{
    const auto & [single_value_size_estimation, size_ratio] = GetParam();

    auto col = ColumnString(single_value_size_estimation);
    size_t total_values_size = 0;

    EXPECT_NO_THROW(col.Reserve(expected_number_of_items * 1.2));
    EXPECT_NO_FATAL_FAILURE(AppendStrings(col, total_values_size));

    const auto max_estimation_error_factor = single_value_size_estimation == ColumnString::NO_PREALLOCATE ? 2.5 : 2;

    // Allow minor overallocations, hence * 1.2
    EXPECT_LT(col.MemoryUsage(), EstimateMemoryUsage(total_values_size, 1.2) * max_estimation_error_factor);
}

/** TODO more tests
 *  "Basic tests":
 *  - first Reserve(), then Append() same number of items that were Reserved()
 *  - first Reserve(), then Append() more items than were Reserved()
 *  - first Reserve(), then Append() less items than were Reserved()
 *
 *  "Extended tests":
 *  - Basic tests, but with items that are smaller than estimated
 *  - Basic tests, but with items that are exactly as estimated
 *  - Basic tests, but with items that are bigger than estimated
 *
 *  "Non-empty column tests":
 *  - same as "Extended tests", but first Append() data below estimated item size
 *  - same as "Extended tests", but first Append() data above estimated item size
 *  - same as "Extended tests", but first Append() data above default block size
 *
 *  "Re-estimation tests": do multiple SetEstimatedValueSize(), Reserve() calls
 *  - first smaller estimation, then larger estimation
 *  - first larger estimation, then smaller estimation
 *  - first no estimation (0), then some estimation
 *
 *  Test all that groups of tests against various valid EstimatedValueSize values.
 */

const auto SIZE_RATIOS = ::testing::ValuesIn(std::initializer_list<SizeRatio>{
    // estimation is about right
    SizeRatio({0.9, 0.95, 1.0, 1.05, 1.1}),
    // estimation is a bit high, real values are about 0.8 of estimated size
    SizeRatio({0.75, 0.8, 0.85}),
    // estimation is a bit low, real values are about 1.2 of estimated size
    SizeRatio({1.25, 1.2, 1.25}),
    // estimation is to high, real values are about 2.0 of estimated size
    SizeRatio({1.9, 2, 2.1}),
    // estimation is to low, real values are about 0.5 of estimated size
    SizeRatio({0.4, 0.5, 0.6}),
});

INSTANTIATE_TEST_SUITE_P(
    NO_PRE_ALLOCATE, ColumnStringEstimatedValueSizeTest,
    ::testing::Combine(
        ::testing::Values(
            ColumnString::NO_PREALLOCATE
        ),
        SIZE_RATIOS
    )
);

INSTANTIATE_TEST_SUITE_P(
    EstimatedValueSize_Values, ColumnStringEstimatedValueSizeTest,
    ::testing::Combine(
        ::testing::Values(
            ColumnString::EstimatedValueSize::TINY,
            ColumnString::EstimatedValueSize::SMALL,
            ColumnString::EstimatedValueSize::MEDIUM,
            ColumnString::EstimatedValueSize::LARGE
        ),
        SIZE_RATIOS
    )
);

// Because whone number of those does't fit in ColumnString::Block of default size,
// there are going to be some unused regions of memory in ColumnString::Block's,
// hitting various corner cases.
INSTANTIATE_TEST_SUITE_P(
    Primes, ColumnStringEstimatedValueSizeTest,
    ::testing::Combine(
        ::testing::Values(
            ColumnString::EstimatedValueSize(3),
            ColumnString::EstimatedValueSize(5),

            ColumnString::EstimatedValueSize(503),
            ColumnString::EstimatedValueSize(509),

            ColumnString::EstimatedValueSize(1009),
            ColumnString::EstimatedValueSize(1013)
        ),
        SIZE_RATIOS
    )
);

INSTANTIATE_TEST_SUITE_P(
    Big, ColumnStringEstimatedValueSizeTest,
    ::testing::Combine(
        ::testing::Values(
            // bigger than 1K
            ColumnString::EstimatedValueSize(4    * 1024),
            ColumnString::EstimatedValueSize(64   * 1024),
            ColumnString::EstimatedValueSize(1024 * 1024),
            ColumnString::EstimatedValueSize(4 * 1024 * 1024)
        ),
        SIZE_RATIOS
    )
);
