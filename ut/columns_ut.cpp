#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>

#include <contrib/gtest/gtest.h>
#include <chrono>

using namespace clickhouse;

static std::vector<uint32_t> MakeNumbers() {
    return std::vector<uint32_t>
        {1, 2, 3, 7, 11, 13, 17, 19, 23, 29, 31};
}

static std::vector<uint8_t> MakeBools() {
    return std::vector<uint8_t>
        {1, 0, 0, 0, 1, 1, 0, 1, 1, 1, 0};
}

static std::vector<std::string> MakeFixedStrings() {
    return std::vector<std::string>
        {"aaa", "bbb", "ccc", "ddd"};
}

static std::vector<std::string> MakeStrings() {
    return std::vector<std::string>
        {"a", "ab", "abc", "abcd"};
}

static std::vector<uint64_t> MakeUUIDs() {
    return std::vector<uint64_t>
        {0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu,
         0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu,
         0x3507213c178649f9llu, 0x9faf035d662f60aellu};
}

// TODO: add tests for ColumnDecimal.

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


TEST(ColumnsCase, ArrayAppend) {
    auto arr1 = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
    auto arr2 = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    arr1->AppendAsColumn(id);

    id->Append(3);
    arr2->AppendAsColumn(id);

    arr1->Append(arr2);

    auto col = arr1->GetAsColumn(1);

    ASSERT_EQ(arr1->Size(), 2u);
    //ASSERT_EQ(col->As<ColumnUInt64>()->At(0), 1u);
    //ASSERT_EQ(col->As<ColumnUInt64>()->At(1), 3u);
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

TEST(ColumnsCase, Date2038) {
    auto col1 = std::make_shared<ColumnDate>();
    std::time_t largeDate(25882ul * 86400ul);
    col1->Append(largeDate);

    ASSERT_EQ(col1->Size(), 1u);
    ASSERT_EQ(static_cast<std::uint64_t>(col1->At(0)), 25882ul * 86400ul);
}

TEST(ColumnsCase, EnumTest) {
    std::vector<Type::EnumItem> enum_items = {{"Hi", 1}, {"Hello", 2}};

    auto col = std::make_shared<ColumnEnum8>(Type::CreateEnum8(enum_items));
    ASSERT_TRUE(col->Type()->IsEqual(Type::CreateEnum8(enum_items)));

    col->Append(1);
    ASSERT_EQ(col->Size(), 1u);
    ASSERT_EQ(col->At(0), 1);
    ASSERT_EQ(col->NameAt(0), "Hi");

    col->Append("Hello");
    ASSERT_EQ(col->Size(), 2u);
    ASSERT_EQ(col->At(1), 2);
    ASSERT_EQ(col->NameAt(1), "Hello");

    auto col16 = std::make_shared<ColumnEnum16>(Type::CreateEnum16(enum_items));
    ASSERT_TRUE(col16->Type()->IsEqual(Type::CreateEnum16(enum_items)));
}

TEST(ColumnsCase, NullableSlice) {
    auto data = std::make_shared<ColumnUInt32>(MakeNumbers());
    auto nulls = std::make_shared<ColumnUInt8>(MakeBools());
    auto col = std::make_shared<ColumnNullable>(data, nulls);
    auto sub = col->Slice(3, 4)->As<ColumnNullable>();
    auto subData = sub->Nested()->As<ColumnUInt32>();

    ASSERT_EQ(sub->Size(), 4u);
    ASSERT_FALSE(sub->IsNull(0));
    ASSERT_EQ(subData->At(0),  7u);
    ASSERT_TRUE(sub->IsNull(1));
    ASSERT_FALSE(sub->IsNull(3));
    ASSERT_EQ(subData->At(3), 17u);
}

TEST(ColumnsCase, UUIDInit) {
    auto col = std::make_shared<ColumnUUID>(std::make_shared<ColumnUInt64>(MakeUUIDs()));

    ASSERT_EQ(col->Size(), 3u);
    ASSERT_EQ(col->At(0), UInt128(0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu));
    ASSERT_EQ(col->At(2), UInt128(0x3507213c178649f9llu, 0x9faf035d662f60aellu));
}

TEST(ColumnsCase, UUIDSlice) {
    auto col = std::make_shared<ColumnUUID>(std::make_shared<ColumnUInt64>(MakeUUIDs()));
    auto sub = col->Slice(1, 2)->As<ColumnUUID>();

    ASSERT_EQ(sub->Size(), 2u);
    ASSERT_EQ(sub->At(0), UInt128(0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu));
    ASSERT_EQ(sub->At(1), UInt128(0x3507213c178649f9llu, 0x9faf035d662f60aellu));
}



std::uint64_t generate(const ColumnUInt64&, size_t index)
{
    const auto base = static_cast<std::uint64_t>(index) % 255;
    return base << 7*8 | base << 6*8 | base << 5*8 | base << 4*8 | base << 3*8 | base << 2*8 | base << 1*8 | base;
}

template <size_t RESULT_SIZE=8>
std::string_view generate_string_view(size_t index)
{
    static const char result_template[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
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

template <typename ChronoDurationType>
struct Timer
{
    using DurationType = ChronoDurationType;

    Timer() {}

    void restart()
    {
        started_at = current();
    }

    void start()
    {
        restart();
    }

    auto elapsed() const
    {
        return std::chrono::duration_cast<ChronoDurationType>(current() - started_at);
    }

    auto current() const
    {
        struct timespec ts;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        return std::chrono::nanoseconds(ts.tv_sec * 1000000000LL + ts.tv_nsec);
    }

private:
    std::chrono::nanoseconds started_at;
};

template <typename ChronoDurationType>
class PausableTimer
{
public:
    PausableTimer()
    {}

    void Start()
    {
        timer.restart();
        paused = false;
    }

    void Pause()
    {
        total += timer.elapsed();
        paused = true;
    }

    auto GetTotalElapsed() const
    {
        if (paused)
        {
            return total;
        }
        else
        {
            return total + timer.elapsed();
        }
    }

    void Reset()
    {
        Pause();
        total = ChronoDurationType{0};
    }

    Timer<ChronoDurationType> timer;
    ChronoDurationType total = ChronoDurationType{0};
    bool paused = false;
};

template <typename ColumnType>
void TestItemLoadingAndSaving(ColumnType && col)
{
    const size_t ITEMS_COUNT = 1'000'000;
    const int LOAD_AND_SAVE_REPEAT_TIMES = 10; // run Load() and Save() multiple times to cancel out measurement errors.

    std::cerr << "\n===========================================================" << std::endl;
    std::cerr << "\t" << ITEMS_COUNT << " items of " << col.Type()->GetName()  << std::endl;

    PausableTimer<std::chrono::microseconds> timer;

    timer.Start();
    for (size_t i = 0; i < ITEMS_COUNT; ++i)
    {
        const auto value = generate(col, i);
        col.Append(value);
    }

    EXPECT_EQ(ITEMS_COUNT, col.Size());
    std::cerr << "Appending:\t" << timer.GetTotalElapsed().count() << " us"
              << std::endl;

    auto validate = [=, &col = std::as_const(col), &timer]()
    {
        timer.Reset();
        timer.Start();

        // validate that appended items match expected
        for (size_t i = 0; i < ITEMS_COUNT; ++i)
        {
            SCOPED_TRACE(i);

            ASSERT_EQ(col.At(i), generate(col, i));
            ASSERT_EQ(col[i], generate(col, i));
        }

        std::cerr << "Accessing (twice):\t" << timer.GetTotalElapsed().count() << " us"
                  << std::endl;
    };

    validate();
    EXPECT_NO_FATAL_FAILURE();

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
            col.Save(&ostr);
            ostr.Flush();
            timer.Pause();
        }
        const auto elapsed = timer.GetTotalElapsed() / (LOAD_AND_SAVE_REPEAT_TIMES * 1.0);

        std::cerr << "Saving:\t" << elapsed.count() << " us"
                  << std::endl;
    }

    // Load
    {
        timer.Reset();

        for (int i = 0; i < LOAD_AND_SAVE_REPEAT_TIMES; ++i)
        {
            ArrayInput arrayInput(buffer.data(), buffer.size());
            CodedInputStream istr(&arrayInput);
            col.Clear();

            timer.Start();
            col.Load(&istr, ITEMS_COUNT);
            timer.Pause();
        }
        const auto elapsed = timer.GetTotalElapsed() / (LOAD_AND_SAVE_REPEAT_TIMES * 1.0);

        std::cerr << "Loading:\t" << elapsed.count() << " us"
                  << std::endl;
    }

    validate();
    EXPECT_NO_FATAL_FAILURE();
}

//// test deserialization of the FixedString column
TEST(ColumnsCase, PERFORMANCE_FixedString) {
    TestItemLoadingAndSaving(ColumnFixedString(8));
}

TEST(ColumnsCase, PERFORMANCE_String) {

    TestItemLoadingAndSaving(ColumnString());
}

TEST(ColumnsCase, PERFORMANCE_Int) {

    TestItemLoadingAndSaving(ColumnUInt64());
}
