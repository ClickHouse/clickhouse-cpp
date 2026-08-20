// Server round-trip tests for LowCardinality over all supported inner types:
//
//     String, FixedString (originally supported)
//     Int8/16/32/64/128, UInt8/16/32/64/128, Float32/64,
//     Date, Date32, DateTime, IPv4, IPv6, UUID (added later)
//
// Every type is tested twice, once as LowCardinality(T) and once as
// LowCardinality(Nullable(T)), with the same scenario:
//
// 1. Create a column of the base type and fill it with values.
// 2. Create a generic ColumnLowCardinality and append the data from the column of step 1.
// 3. Send the column of step 2 through a ClickHouse server (INSERT + SELECT) and
//    convert the returned column to ColumnLowCardinalityT<TheType>.
// 4. Expect the data of step 1 to be equal to the data of step 3.

#include <clickhouse/client.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>

#include <gtest/gtest.h>

#include "ut/roundtrip_column.h"
#include "ut/utils.h"
#include "ut/utils_comparison.h"
#include "ut/value_generators.h"

#include <algorithm>
#include <cmath>
#include <ctime>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

namespace {

using namespace clickhouse;

// ColumnDate32 (unlike ColumnDate) also supports pre-epoch dates,
// extend the common Date values with one.
std::vector<std::time_t> MakeDates32AsSeconds() {
    auto result = MakeDates<std::time_t>();
    result.push_back(std::time_t(-2) * 86400);
    return result;
}

// A single test case: base column type + value generator (+ optional column
// constructor arguments, e.g. the width of a FixedString).
template <typename ColumnType, auto ValueGenerator, size_t ...ConstructorArgs>
struct TestCase {
    using BaseColumn = ColumnType;

    static auto MakeColumn() { return std::make_shared<ColumnType>(ConstructorArgs...); }

    static auto MakeValues() {
        auto values = ValueGenerator();

        // The floating-point generators produce NaNs, which never compare
        // equal to themselves, drop them to keep plain EXPECT_EQ verification.
        using ValueType = typename decltype(values)::value_type;
        if constexpr (std::is_floating_point_v<ValueType>) {
            values.erase(std::remove_if(
                    values.begin(),
                    values.end(),
                    [](ValueType v) {
                        return std::isnan(v);
                    }),
                    values.end());
        }

        // Duplicate a value so that the dictionary/deduplication code path is exercised too.
        values.push_back(values.front());
        return values;
    }
};

} // namespace

template <typename Case>
class LowCardinalityTypedTest : public ::testing::Test {
protected:
    void SetUp() override {
        client_ = std::make_unique<Client>(
            ClientOptions()
                .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
                .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
                .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
                .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
                .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"))
                .SetPingBeforeQuery(true));
    }

    void TearDown() override {
        client_.reset();
    }

    std::unique_ptr<Client> client_;
};

using LowCardinalityTestCases = ::testing::Types<
    TestCase<ColumnString, &MakeStrings>,
    TestCase<ColumnFixedString, &MakeFixedStrings<4>, 4>,

    TestCase<ColumnInt8,  &MakeNumbers<int8_t>>,
    TestCase<ColumnInt16, &MakeNumbers<int16_t>>,
    TestCase<ColumnInt32, &MakeNumbers<int32_t>>,
    TestCase<ColumnInt64, &MakeNumbers<int64_t>>,

    TestCase<ColumnUInt8,  &MakeNumbers<uint8_t>>,
    TestCase<ColumnUInt16, &MakeNumbers<uint16_t>>,
    TestCase<ColumnUInt32, &MakeNumbers<uint32_t>>,
    TestCase<ColumnUInt64, &MakeNumbers<uint64_t>>,

    TestCase<ColumnInt128,  &MakeInt128s>,
    TestCase<ColumnUInt128, &MakeUInt128s>,

    TestCase<ColumnFloat32, &MakeNumbers<float>>,
    TestCase<ColumnFloat64, &MakeNumbers<double>>,

    TestCase<ColumnDate,     &MakeDates<std::time_t>>,
    TestCase<ColumnDate32,   &MakeDates32AsSeconds>,
    TestCase<ColumnDateTime, &MakeDateTimes>,

    TestCase<ColumnIPv4, &MakeIPv4s>,
    TestCase<ColumnIPv6, &MakeIPv6s>,
    TestCase<ColumnUUID, &MakeUUIDs>
>;

TYPED_TEST_SUITE(LowCardinalityTypedTest, LowCardinalityTestCases);

// LowCardinality(T)
TYPED_TEST(LowCardinalityTypedTest, RoundtripAndReadThroughTypedView) {
    using Case = TypeParam;
    using BaseColumn = typename Case::BaseColumn;
    using TypedLowCardinality = ColumnLowCardinalityT<BaseColumn>;

    // Step 1: base column with values.
    auto source = Case::MakeColumn();
    for (const auto& value : Case::MakeValues()) {
        source->Append(value);
    }
    ASSERT_GT(source->Size(), 0u);

    // Step 2: generic LowCardinality column, append data of the base column.
    auto low_cardinality = std::make_shared<ColumnLowCardinality>(Case::MakeColumn());
    low_cardinality->Append(source);
    ASSERT_EQ(source->Size(), low_cardinality->Size());

    // Step 3: send through the server (INSERT + SELECT) and convert the
    // returned column to typed ColumnLowCardinalityT<BaseColumn>.
    auto returned = RoundtripColumnValues(*this->client_, low_cardinality);
    auto typed = returned->template AsStrict<TypedLowCardinality>();
    ASSERT_EQ(source->Size(), typed->Size());

    // Step 4: data of step 1 must equal data of step 3.
    for (size_t i = 0; i < source->Size(); ++i) {
        SCOPED_TRACE(::testing::Message("at row ") << i);
        EXPECT_EQ(source->At(i), typed->At(i));
        EXPECT_EQ((*source)[i], (*typed)[i]);
    }
}

// LowCardinality(Nullable(T))
TYPED_TEST(LowCardinalityTypedTest, RoundtripNullableAndReadThroughTypedView) {
    using Case = TypeParam;
    using BaseColumn = typename Case::BaseColumn;
    using NullableColumn = ColumnNullableT<BaseColumn>;
    using TypedLowCardinality = ColumnLowCardinalityT<NullableColumn>;

    // Step 1: nullable base column with values interleaved with NULLs.
    auto source = std::make_shared<NullableColumn>(Case::MakeColumn());
    for (const auto& value : Case::MakeValues()) {
        source->Append(typename NullableColumn::ValueType{value});
        source->Append(std::nullopt);
    }
    ASSERT_GT(source->Size(), 0u);

    // Step 2: generic LowCardinality column over a Nullable dictionary,
    // append data of the nullable column.
    auto nullable = std::make_shared<NullableColumn>(Case::MakeColumn());
    auto low_cardinality = std::make_shared<ColumnLowCardinality>(nullable);
    low_cardinality->Append(source);
    ASSERT_EQ(source->Size(), low_cardinality->Size());

    // Step 3: send through the server (INSERT + SELECT) and convert the
    // returned column to typed ColumnLowCardinalityT<ColumnNullableT<BaseColumn>>.
    auto returned = RoundtripColumnValues(*this->client_, low_cardinality);
    auto typed = returned->template AsStrict<TypedLowCardinality>();
    ASSERT_EQ(source->Size(), typed->Size());

    // Step 4: data of step 1 must equal data of step 3, NULLs included.
    for (size_t i = 0; i < source->Size(); ++i) {
        SCOPED_TRACE(::testing::Message("at row ") << i);

        const auto expected = source->At(i);
        const auto actual = typed->At(i);

        ASSERT_EQ(expected.has_value(), actual.has_value());
        if (expected.has_value()) {
            EXPECT_EQ(*expected, *actual);
        }
    }
}
