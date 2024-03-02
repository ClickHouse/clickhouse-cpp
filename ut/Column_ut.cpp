#include <clickhouse/columns/array.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/socket.h> // for ipv4-ipv6 platform-specific stuff

#include <clickhouse/client.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <initializer_list>
#include <memory>
#include <type_traits>

#include "gtest/internal/gtest-internal.h"
#include "ut/utils_comparison.h"
#include "ut/utils_meta.h"
#include "utils.h"
#include "roundtrip_column.h"
#include "value_generators.h"

namespace {
using namespace clickhouse;
}

namespace clickhouse{

std::ostream& operator<<(std::ostream& ostr, const Type::Code& type_code) {
    return ostr << Type::TypeName(type_code) << " (" << static_cast<int>(type_code) << ")";
}

}


// Generic tests for a Column subclass against basic API:
// 1. Constructor: Create, ensure that it is empty
// 2. Append: Create, add some data one by one via Append, make sure that values inserted match extracted with At() and operator[]
// 3. Slice: Create, add some data via Append, do Slice()
// 4. CloneEmpty Create, invoke CloneEmplty, ensure that clone is Empty
// 5. Clear: Create, add some data, invoke Clear(), make sure column is empty
// 6. Swap: create two instances, populate one with data, swap with second, make sure has data was transferred
// 7. Load/Save: create, append some data, save to buffer, load from same buffer into new column, make sure columns match.

template <typename ColumnTypeT,
          typename std::shared_ptr<ColumnTypeT> (*CreatorFunction)(),
          typename GeneratorValueType,
          typename std::vector<GeneratorValueType> (*GeneratorFunction)()>
struct GenericColumnTestCase
{
    using ColumnType = ColumnTypeT;
    static constexpr auto Creator = CreatorFunction;
    static constexpr auto Generator = GeneratorFunction;

    static auto createColumn()
    {
        return CreatorFunction();
    }

    static auto generateValues()
    {
        return GeneratorFunction();
    }
};

template <typename T>
class GenericColumnTest : public testing::Test {
public:
    using ColumnType = typename T::ColumnType;

    static auto MakeColumn()
    {
        return T::createColumn();
    }

    static auto GenerateValues(size_t values_size)
    {
        return GenerateVector(values_size, FromVectorGenerator{T::generateValues()});
    }

    template <typename Values>
    static void AppendValues(std::shared_ptr<ColumnType> column, const Values& values) {
        for (const auto & v : values) {
            column->Append(v);
        }
    }

    static auto MakeColumnWithValues(size_t values_size) {
        auto column = MakeColumn();
        auto values = GenerateValues(values_size);
        AppendValues(column, values);

        return std::tuple{column, values};
    }

    static std::optional<std::string> CheckIfShouldSkipTest(clickhouse::Client& client) {
        if constexpr (std::is_same_v<ColumnType, ColumnDate32>) {
            // Date32 first appeared in v21.9.2.17-stable
            const auto server_info = client.GetServerInfo();
            if (versionNumber(server_info) < versionNumber(21, 9)) {
                std::stringstream buffer;
                buffer << "Date32 is available since v21.9.2.17-stable and can't be tested against server: " << server_info;
                return buffer.str();
            }
        }

        if constexpr (std::is_same_v<ColumnType, ColumnInt128>) {
            const auto server_info = client.GetServerInfo();
            if (versionNumber(server_info) < versionNumber(21, 7)) {
                std::stringstream buffer;
                buffer <<  "ColumnInt128 is available since v21.7.2.7-stable and can't be tested against server: " << server_info;
                return buffer.str();
            }
        }
        return std::nullopt;
    }

    template <typename ColumnType>
    static void TestColumnRoundtrip(const std::shared_ptr<ColumnType> & column, const ClientOptions & client_options)
    {
        SCOPED_TRACE(::testing::Message("Column type: ") << column->GetType().GetName());
        SCOPED_TRACE(::testing::Message("Client options: ") << client_options);

        clickhouse::Client client(client_options);

        if (auto message = CheckIfShouldSkipTest(client)) {
            GTEST_SKIP() << *message;
        }

        auto result_typed = RoundtripColumnValues(client, column)->template AsStrict<ColumnType>();
        EXPECT_TRUE(CompareRecursive(*column, *result_typed));
    }


    template <typename ColumnType, typename CompressionMethods>
    static void TestColumnRoundtrip(const ColumnType & column, const ClientOptions & client_options, CompressionMethods && compression_methods)
    {
        for (auto compressionMethod : compression_methods)
        {
            ClientOptions new_options = ClientOptions(client_options).SetCompressionMethod(compressionMethod);
            TestColumnRoundtrip(column, new_options);
        }
    }
};

// Luckily all (non-data copying/moving) constructors have size_t params.
template <typename ColumnType, size_t ...ConstructorParams>
auto makeColumn()
{
    return std::make_shared<ColumnType>(ConstructorParams...);
}

template <typename ColumnTypeT>
struct NumberColumnTestCase : public GenericColumnTestCase<ColumnTypeT, &makeColumn<ColumnTypeT>, typename ColumnTypeT::ValueType, &MakeNumbers<typename ColumnTypeT::ValueType>>
{
    using Base = GenericColumnTestCase<ColumnTypeT, &makeColumn<ColumnTypeT>, typename ColumnTypeT::ValueType, &MakeNumbers<typename ColumnTypeT::ValueType>>;

    using ColumnType = typename Base::ColumnType;
    using Base::createColumn;
    using Base::generateValues;
};

template <typename ColumnTypeT, size_t precision, size_t scale>
struct DecimalColumnTestCase : public GenericColumnTestCase<ColumnDecimal, &makeColumn<ColumnDecimal, precision, scale>, clickhouse::Int128, &MakeDecimals<precision, scale>>
{
    using Base = GenericColumnTestCase<ColumnDecimal, &makeColumn<ColumnDecimal, precision, scale>, clickhouse::Int128, &MakeDecimals<precision, scale>>;

    using ColumnType = typename Base::ColumnType;
    using Base::createColumn;
    using Base::generateValues;
};

using TestCases = ::testing::Types<
    NumberColumnTestCase<ColumnUInt8>,
    NumberColumnTestCase<ColumnUInt16>,
    NumberColumnTestCase<ColumnUInt32>,
    NumberColumnTestCase<ColumnUInt64>,

    NumberColumnTestCase<ColumnInt8>,
    NumberColumnTestCase<ColumnInt16>,
    NumberColumnTestCase<ColumnInt32>,
    NumberColumnTestCase<ColumnInt64>,

    NumberColumnTestCase<ColumnFloat32>,
    NumberColumnTestCase<ColumnFloat64>,

    GenericColumnTestCase<ColumnString, &makeColumn<ColumnString>, std::string, &MakeStrings>,
    GenericColumnTestCase<ColumnFixedString, &makeColumn<ColumnFixedString, 12>, std::string, &MakeFixedStrings<12>>,

    GenericColumnTestCase<ColumnDate, &makeColumn<ColumnDate>, time_t, &MakeDates<time_t>>,
    GenericColumnTestCase<ColumnDate32, &makeColumn<ColumnDate32>, time_t, &MakeDates<time_t>>,
    GenericColumnTestCase<ColumnDateTime, &makeColumn<ColumnDateTime>, clickhouse::Int64, &MakeDateTimes>,
    GenericColumnTestCase<ColumnDateTime64, &makeColumn<ColumnDateTime64, 0>, clickhouse::Int64, &MakeDateTime64s<0>>,
    GenericColumnTestCase<ColumnDateTime64, &makeColumn<ColumnDateTime64, 3>, clickhouse::Int64, &MakeDateTime64s<3>>,
    GenericColumnTestCase<ColumnDateTime64, &makeColumn<ColumnDateTime64, 6>, clickhouse::Int64, &MakeDateTime64s<6>>,
    GenericColumnTestCase<ColumnDateTime64, &makeColumn<ColumnDateTime64, 9>, clickhouse::Int64, &MakeDateTime64s<9>>,

    GenericColumnTestCase<ColumnIPv4, &makeColumn<ColumnIPv4>, in_addr, &MakeIPv4s>,
    GenericColumnTestCase<ColumnIPv6, &makeColumn<ColumnIPv6>, in6_addr, &MakeIPv6s>,

    GenericColumnTestCase<ColumnInt128, &makeColumn<ColumnInt128>, clickhouse::Int128, &MakeInt128s>,
    GenericColumnTestCase<ColumnUUID, &makeColumn<ColumnUUID>, clickhouse::UUID, &MakeUUIDs>,

    DecimalColumnTestCase<ColumnDecimal, 18, 0>,
    DecimalColumnTestCase<ColumnDecimal, 18, 6>,
    DecimalColumnTestCase<ColumnDecimal, 18, 12>,
    // there is an arithmetical overflow on some values in the test value generator harness
    // DecimalColumnTestCase<ColumnDecimal, 18, 15>,

    DecimalColumnTestCase<ColumnDecimal, 12, 0>,
    DecimalColumnTestCase<ColumnDecimal, 12, 6>,
    DecimalColumnTestCase<ColumnDecimal, 12, 9>,

    DecimalColumnTestCase<ColumnDecimal, 6, 0>,
    DecimalColumnTestCase<ColumnDecimal, 6, 3>,

    GenericColumnTestCase<ColumnLowCardinalityT<ColumnString>, &makeColumn<ColumnLowCardinalityT<ColumnString>>, std::string, &MakeStrings>

    // Array(String)
//    GenericColumnTestCase<ColumnArrayT<ColumnString>, &makeColumn<ColumnArrayT<ColumnString>>, std::vector<std::string>, &MakeArrays<std::string, &MakeStrings>>

//    // Array(Array(String))
//    GenericColumnTestCase<ColumnArrayT<ColumnArrayT<ColumnString>>, &makeColumn<ColumnArrayT<ColumnArrayT<ColumnString>>>,
//            std::vector<std::vector<std::string>>,
//            &MakeArrays<std::vector<std::string>, &MakeArrays<std::string, &MakeStrings>>>
    >;

TYPED_TEST_SUITE(GenericColumnTest, TestCases);

TYPED_TEST(GenericColumnTest, Construct) {
    auto column = this->MakeColumn();
    ASSERT_EQ(0u, column->Size());
}

TYPED_TEST(GenericColumnTest, EmptyColumn) {
    auto column = this->MakeColumn();
    ASSERT_EQ(0u, column->Size());

    // verify that Column methods work as expected on empty column:
    // some throw exceptions, some return poper values (like CloneEmpty)

    // Shouldn't be able to get items on empty column.
    ASSERT_ANY_THROW(column->At(0));

    {
        auto slice = column->Slice(0, 0);
        ASSERT_NO_THROW(slice->template AsStrict<typename TestFixture::ColumnType>());
        ASSERT_EQ(0u, slice->Size());
    }

    {
        auto clone = column->CloneEmpty();
        ASSERT_NO_THROW(clone->template AsStrict<typename TestFixture::ColumnType>());
        ASSERT_EQ(0u, clone->Size());
    }

    ASSERT_NO_THROW(column->Clear());
    ASSERT_NO_THROW(column->Swap(*this->MakeColumn()));
}

TYPED_TEST(GenericColumnTest, Append) {
    auto column = this->MakeColumn();
    const auto values = this->GenerateValues(10'000);

    for (const auto & v : values) {
        EXPECT_NO_THROW(column->Append(v));
    }

    EXPECT_TRUE(CompareRecursive(values, *column));
}

// To make some value types compatitable with Column::GetItem()
template <typename ColumnType, typename ValueType>
inline auto convertValueForGetItem(const ColumnType& col, ValueType&& t) {
    using T = std::remove_cv_t<std::decay_t<ValueType>>;

    if constexpr (std::is_same_v<ColumnType, ColumnDecimal>) {
        // Since ColumnDecimal can hold 32, 64, 128-bit wide data and there is no way telling at run-time.
        const ItemView item = col.GetItem(0);
        return std::string_view(reinterpret_cast<const char*>(&t), item.data.size());
    } else if constexpr (std::is_same_v<T, clickhouse::UInt128>
            || std::is_same_v<T, clickhouse::Int128>) {
        return std::string_view{reinterpret_cast<const char*>(&t), sizeof(T)};
    } else if constexpr (std::is_same_v<T, in_addr>) {
        return htonl(t.s_addr);
    } else if constexpr (std::is_same_v<T, in6_addr>) {
        return std::string_view(reinterpret_cast<const char*>(t.s6_addr), 16);
    } else if constexpr (std::is_same_v<ColumnType, ColumnDate>) {
        return static_cast<uint16_t>(t / std::time_t(86400));
    } else if constexpr (std::is_same_v<ColumnType, ColumnDate32>) {
        return static_cast<uint32_t>(t / std::time_t(86400));
    } else if constexpr (std::is_same_v<ColumnType, ColumnDateTime>) {
        return static_cast<uint32_t>(t);
    } else {
        return t;
    }
}

TYPED_TEST(GenericColumnTest, GetItem) {
    auto [column, values] = this->MakeColumnWithValues(10'000);

    ASSERT_EQ(values.size(), column->Size());
    const auto wrapping_types = std::set<Type::Code>{
        Type::Code::LowCardinality, Type::Code::Array, Type::Code::Nullable
    };

    // For wrapping types, type of ItemView can be different from type of column
    if (wrapping_types.find(column->GetType().GetCode()) == wrapping_types.end() ) {
        EXPECT_EQ(column->GetItem(0).type, column->GetType().GetCode());
    }

    for (size_t i = 0; i < values.size(); ++i) {
        const auto v = convertValueForGetItem(*column, values[i]);
        const ItemView item = column->GetItem(i);

        ASSERT_TRUE(CompareRecursive(v, item.get<decltype(v)>()))
            << " On item " << i << " of " << PrintContainer{values};
    }
}

TYPED_TEST(GenericColumnTest, Slice) {
    auto [column, values] = this->MakeColumnWithValues(10'000);

    auto untyped_slice = column->Slice(0, column->Size());
    auto slice = untyped_slice->template AsStrict<typename TestFixture::ColumnType>();
    EXPECT_EQ(column->GetType(), slice->GetType());

    EXPECT_TRUE(CompareRecursive(values, *slice));

    // TODO: slices of different sizes
}

TYPED_TEST(GenericColumnTest, CloneEmpty) {
    auto [column, values] = this->MakeColumnWithValues(10'000);
    EXPECT_EQ(values.size(), column->Size());

    auto clone_untyped = column->CloneEmpty();
    // Check that type matches
    auto clone = clone_untyped->template AsStrict<typename TestFixture::ColumnType>();
    EXPECT_EQ(0u, clone->Size());

    EXPECT_EQ(column->GetType(), clone->GetType());
}

TYPED_TEST(GenericColumnTest, Clear) {
    auto [column, values] = this->MakeColumnWithValues(10'000);
    EXPECT_EQ(values.size(), column->Size());

    column->Clear();
    EXPECT_EQ(0u, column->Size());
}

TYPED_TEST(GenericColumnTest, Swap) {
    auto [column_A, values] = this->MakeColumnWithValues(10'000);
    auto column_B = this->MakeColumn();

    column_A->Swap(*column_B);

    EXPECT_EQ(0u, column_A->Size());
    EXPECT_TRUE(CompareRecursive(values, *column_B));
}

// GTEST_SKIP for debug builds to draw attention of developer
#if !defined(NDEBUG)
#define COLUMN_DOESNT_IMPLEMENT(comment) GTEST_SKIP() << this->MakeColumn()->GetType().GetName() << " doesn't implement " << comment;
#else
#define COLUMN_DOESNT_IMPLEMENT(comment) GTEST_SUCCEED() << this->MakeColumn()->GetType().GetName() << " doesn't implement " << comment;
#endif

TYPED_TEST(GenericColumnTest, ReserveAndCapacity) {
    using column_type = typename TestFixture::ColumnType;
    auto [column0, values] = this->MakeColumnWithValues(2);
    auto values_copy = values;
    EXPECT_NO_THROW(column0->Reserve(0u));
    EXPECT_EQ(2u, column0->Size());
    EXPECT_TRUE(CompareRecursive(values, values_copy));

    auto column1 = this->MakeColumn();
    column1->Reserve(10u);
    EXPECT_EQ(0u, column1->Size());

    if constexpr (has_method_Reserve_v<column_type> && has_method_Capacity_v<column_type>) {
        auto column = this->MakeColumn();
        EXPECT_EQ(0u, column->Capacity());
        EXPECT_NO_THROW(column->Reserve(100u));
        EXPECT_EQ(100u, column->Capacity());
        EXPECT_EQ(0u, column->Size());
    }
    else {
        COLUMN_DOESNT_IMPLEMENT("method Reserve() and Capacity()");
    }
}


TYPED_TEST(GenericColumnTest, GetWritableData) {
    if constexpr (has_method_GetWritableData_v<typename TestFixture::ColumnType>) {
        auto [column, values] = this->MakeColumnWithValues(111);
        // Do conversion from time_t to internal representation, similar to what ColumnDate and ColumnDate32 do
        if constexpr (is_one_of_v<typename TestFixture::ColumnType,
                    ColumnDate,
                    ColumnDate32>) {
            std::for_each(values.begin(), values.end(), [](auto & value) {
                value /= 86400;
            });
        }

        EXPECT_TRUE(CompareRecursive(values, column->GetWritableData()));
    }
    else {
        COLUMN_DOESNT_IMPLEMENT("method GetWritableData()");
    }
}


TYPED_TEST(GenericColumnTest, LoadAndSave) {
    auto [column_A, values] = this->MakeColumnWithValues(100);

    // large buffer since we have pretty big values for String column
    auto const BufferSize = 10*1024*1024;
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(BufferSize);
    memset(buffer.get(), 0, BufferSize);
    {
        ArrayOutput output(buffer.get(), BufferSize);
        // Save
        ASSERT_NO_THROW(column_A->Save(&output));
    }

    auto column_B = this->MakeColumn();
    {
        ArrayInput input(buffer.get(), BufferSize);
        // Load
        ASSERT_TRUE(column_B->Load(&input, values.size()));
    }

    EXPECT_TRUE(CompareRecursive(*column_A, *column_B));
}

const auto LocalHostEndpoint = ClientOptions()
        .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
        .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
        .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
        .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));

const auto AllCompressionMethods = {
    clickhouse::CompressionMethod::None,
    clickhouse::CompressionMethod::LZ4,
    clickhouse::CompressionMethod::ZSTD
};

TYPED_TEST(GenericColumnTest, RoundTrip) {
    auto [column, values] = this->MakeColumnWithValues(10'000);
    EXPECT_EQ(values.size(), column->Size());

    this->TestColumnRoundtrip(column, LocalHostEndpoint, AllCompressionMethods);
}

TYPED_TEST(GenericColumnTest, NullableT_RoundTrip) {
    using NullableType = ColumnNullableT<typename TestFixture::ColumnType>;

    auto non_nullable_column = this->MakeColumn();
    if (non_nullable_column->GetType().GetCode() == Type::Code::LowCardinality)
        // TODO (vnemkov): wrap as ColumnLowCardinalityT<ColumnNullableT<NestedColumn>> instead of ColumnNullableT<ColumnLowCardinalityT<NestedColumn>>
        GTEST_SKIP() << "Can't have " << non_nullable_column->GetType().GetName() << " in Nullable";

    auto column = std::make_shared<NullableType>(std::move(non_nullable_column));
    auto values = this->GenerateValues(10'000);

    FromVectorGenerator<bool> is_null({true, false});
    for (size_t i = 0; i < values.size(); ++i) {
        if (is_null(i)) {
            column->Append(std::nullopt);
        } else {
            column->Append(values[i]);
        }
    }

    this->TestColumnRoundtrip(column, LocalHostEndpoint, AllCompressionMethods);
}

TYPED_TEST(GenericColumnTest, ArrayT_RoundTrip) {
    using ColumnArrayType = ColumnArrayT<typename TestFixture::ColumnType>;

    auto [nested_column, values] = this->MakeColumnWithValues(100);

    auto column = std::make_shared<ColumnArrayType>(nested_column->CloneEmpty()->template As<typename TestFixture::ColumnType>());
    for (size_t i = 0; i < values.size(); ++i)
    {
        const std::vector<std::decay_t<decltype(values[0])>> row{values.begin(), values.begin() + i};
        column->Append(values.begin(), values.begin() + i);

        EXPECT_TRUE(CompareRecursive(row, (*column)[column->Size() - 1]));
    }
    EXPECT_EQ(values.size(), column->Size());

    this->TestColumnRoundtrip(column, LocalHostEndpoint, AllCompressionMethods);
}

