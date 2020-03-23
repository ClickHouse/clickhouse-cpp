#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/client.h>

#include <contrib/gtest/gtest.h>

#include <string>

#include "utils.h"

using namespace clickhouse;

std::uint64_t generate(const ColumnUInt64&, size_t index)
{
    const auto base = static_cast<std::uint64_t>(index) % 255;
    return base << 7*8 | base << 6*8 | base << 5*8 | base << 4*8 | base << 3*8 | base << 2*8 | base << 1*8 | base;
}

template <size_t RESULT_SIZE=8>
std::string_view generate_string_view(size_t index)
{
    static const char result_template[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                                          "9876543210ZYXWVUTSRQPONMLKJIHGFEDCBAzyxwvutsrqponmlkjihgfedcba"; // to double number of unique combinations
    const auto template_size = sizeof(result_template) - 1;

    const auto start_pos = index % (template_size - RESULT_SIZE);
    return std::string_view(&result_template[start_pos], RESULT_SIZE);
}

std::string_view generate(const ColumnString&, size_t index)
{
    // ColumString stores item lengts,and on 1M etnries that builds up to extra 1M bytes,
    // comparing to 8M bytes of serialized data for ColumnFixedString and ColumUInt64.
    // So in order to make comparison mode fair, reducing size of data item.
    return generate_string_view<7>(index);
}

std::string_view generate(const ColumnFixedString&, size_t index)
{
    return generate_string_view<8>(index);
}

std::string_view generate(const ColumnLowCardinalityT<ColumnString>&, size_t index)
{
    return generate_string_view<7>(index);
}

std::string_view generate(const ColumnLowCardinalityT<ColumnFixedString>&, size_t index)
{
    return generate_string_view<8>(index);
}

template <typename ColumnType>
auto ValidateColumnItems(ColumnType && col, size_t expected_items)
{
    ASSERT_EQ(expected_items, col.Size());
    // validate that appended items match expected
    for (size_t i = 0; i < expected_items; ++i)
    {
        SCOPED_TRACE(i);

        ASSERT_EQ(col.At(i), generate(col, i));
        ASSERT_EQ(col[i], generate(col, i));
    }
};

//template <typename ColumnType>
//std::shared_ptr<ColumnType> GetColumnAs(ColumnRef col) {
//    return col->As<ColumnType>();
//}

//template <>
//std::shared_ptr<ColumnLowCardinalityT<ColumnString>>
//GetColumnAs<ColumnLowCardinalityT<ColumnString>>(ColumnRef col) {
//    return std::make_shared<ColumnLowCardinalityT<ColumnString>>(col->As<ColumnLowCardinality>());
//}

//template <>
//std::shared_ptr<ColumnLowCardinalityT<ColumnFixedString>>
//GetColumnAs<ColumnLowCardinalityT<ColumnFixedString>>(ColumnRef col) {
//    return std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(col->As<ColumnLowCardinality>());
//}

template <typename ColumnType>
ColumnType InstantiateColumn() {
    if constexpr (std::is_same_v<ColumnType, ColumnFixedString>) {
        return ColumnType(8);
    }
    else if constexpr (std::is_same_v<ColumnType, ColumnLowCardinalityT<ColumnFixedString>>) {
        return ColumnType(8);
    }
    else {
        return ColumnType();
    }
}

template <typename ColumnType>
class ColumnPerformanceTest : public ::testing::Test {
};

TYPED_TEST_CASE_P(ColumnPerformanceTest);

TYPED_TEST_P(ColumnPerformanceTest, SaveAndLoad) {
    auto column = InstantiateColumn<TypeParam>();

    const size_t ITEMS_COUNT = 1'000'000;
    const int LOAD_AND_SAVE_REPEAT_TIMES = 10; // run Load() and Save() multiple times to cancel out measurement errors.

    std::cerr << "\n===========================================================" << std::endl;
    std::cerr << "\t" << ITEMS_COUNT << " items of " << column.Type()->GetName()  << std::endl;

    PausableTimer<std::chrono::microseconds> timer;

    timer.Start();
    for (size_t i = 0; i < ITEMS_COUNT; ++i)
    {
        const auto value = generate(column, i);
        column.Append(value);
    }

    EXPECT_EQ(ITEMS_COUNT, column.Size());
    std::cerr << "Appending:\t" << timer.GetTotalElapsed().count() << " us"
              << std::endl;

    timer.Reset();
    timer.Start();
    EXPECT_NO_FATAL_FAILURE(ValidateColumnItems(column, ITEMS_COUNT));
    std::cerr << "Accessing (twice):\t" << timer.GetTotalElapsed().count() << " us"
              << std::endl;

    Buffer buffer;

    // Save
    {
        timer.Reset();

        for (int i = 0; i < LOAD_AND_SAVE_REPEAT_TIMES; ++i)
        {
            buffer.clear();
            BufferOutput bufferOutput(&buffer);
            CodedOutputStream ostr(&bufferOutput);

            timer.Start();
            column.Save(&ostr);
            ostr.Flush();
            timer.Pause();
        }
        const auto elapsed = timer.GetTotalElapsed() / (LOAD_AND_SAVE_REPEAT_TIMES * 1.0);

        std::cerr << "Saving:\t" << elapsed.count() << " us"
                  << std::endl;
    }

    std::cerr << "Serialized binary size: " << buffer.size() << std::endl;

    // Load
    {
        timer.Reset();

        for (int i = 0; i < LOAD_AND_SAVE_REPEAT_TIMES; ++i)
        {
            ArrayInput arrayInput(buffer.data(), buffer.size());
            CodedInputStream istr(&arrayInput);
            column.Clear();

            timer.Start();
            column.Load(&istr, ITEMS_COUNT);
            timer.Pause();
        }
        const auto elapsed = timer.GetTotalElapsed() / (LOAD_AND_SAVE_REPEAT_TIMES * 1.0);

        std::cerr << "Loading:\t" << elapsed.count() << " us"
                  << std::endl;
    }

    EXPECT_NO_FATAL_FAILURE(ValidateColumnItems(column, ITEMS_COUNT));
}

TYPED_TEST_P(ColumnPerformanceTest, InsertAndSelect) {
    using ColumnType = TypeParam;

    const std::string table_name = "PerformanceTests.ColumnTest";
    const std::string column_name = "column";

    auto column = InstantiateColumn<ColumnType>();
    Client client(ClientOptions().SetHost("localhost"));
    client.Execute("CREATE DATABASE IF NOT EXISTS PerformanceTests");
    client.Execute("DROP TABLE IF EXISTS PerformanceTests.ColumnTest");
    client.Execute("CREATE TABLE PerformanceTests.ColumnTest (" + column_name + " " + column.Type()->GetName() + ") ENGINE = Memory");

    const size_t ITEMS_COUNT = 1'000'000;

    std::cerr << "\n===========================================================" << std::endl;
    std::cerr << "\t" << ITEMS_COUNT << " items of " << column.Type()->GetName()  << std::endl;

    PausableTimer<std::chrono::microseconds> timer;

    timer.Restart();
    for (size_t i = 0; i < ITEMS_COUNT; ++i)
    {
        const auto value = generate(column, i);
        column.Append(value);
    }

    EXPECT_EQ(ITEMS_COUNT, column.Size());
    std::cerr << "Appending:\t" << timer.GetTotalElapsed().count() << " us"
        << std::endl;

    EXPECT_NO_FATAL_FAILURE(ValidateColumnItems(column, ITEMS_COUNT));

    {
        Block block;
        block.AppendColumn(column_name, column.Slice(0, ITEMS_COUNT));

        timer.Restart();
        client.Insert(table_name, block);
        std::cerr << "INSERT:\t" << timer.GetTotalElapsed().count() << " us"
            << std::endl;
    }

    {
        timer.Restart();
        client.Select("SELECT " + column_name +  " FROM " + table_name, [&](const Block & block) {
            if (block.GetRowCount() == 0) {
                return;
            }

            EXPECT_EQ(1u, block.GetColumnCount());
            const auto col = block[0]->As<ColumnType>();

            EXPECT_NO_FATAL_FAILURE(ValidateColumnItems(*col, ITEMS_COUNT));
        });

        std::cerr << "SELECT:\t" << timer.GetTotalElapsed().count() << " us"
            << std::endl;
    }
}

REGISTER_TYPED_TEST_CASE_P(ColumnPerformanceTest,
    SaveAndLoad, InsertAndSelect);

using SimpleColumnTypes = testing::Types<ColumnUInt64, ColumnString, ColumnFixedString>;
INSTANTIATE_TYPED_TEST_CASE_P(SimpleColumns, ColumnPerformanceTest, SimpleColumnTypes);

using LowCardinalityColumnTypes = ::testing::Types<ColumnLowCardinalityT<ColumnString>, ColumnLowCardinalityT<ColumnFixedString>>;
INSTANTIATE_TYPED_TEST_CASE_P(LowCardinality, ColumnPerformanceTest, LowCardinalityColumnTypes);
