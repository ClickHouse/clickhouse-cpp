#include <clickhouse/client.h>

#include "utils.h"
#include "roundtrip_column.h"

#include <gtest/gtest.h>
#include <map>
#include <optional>

using namespace clickhouse;

// Use value-parameterized tests to run same tests with different client
// options.
class RoundtripCase : public testing::TestWithParam<ClientOptions> {
protected:
    void SetUp() override {
        client_ = std::make_unique<Client>(GetParam());
    }

    void TearDown() override {
        client_.reset();
    }

    std::string GetSettingValue(const std::string& name) {
        std::string result;
        client_->Select("SELECT value FROM system.settings WHERE name = \'" + name + "\'",
                [&result](const Block& block)
            {
                if (block.GetRowCount() == 0) {
                    return;
                }
                result = block[0]->AsStrict<ColumnString>()->At(0);
            }
        );
        return result;
    }


    std::unique_ptr<Client> client_;
};

TEST_P(RoundtripCase, ArrayTUint64) {
    auto array = std::make_shared<ColumnArrayT<ColumnUInt64>>();
    array->Append({0, 1, 2});

    auto result = RoundtripColumnValues(*client_, array)->AsStrict<ColumnArray>();
    auto row = result->GetAsColumn(0)->As<ColumnUInt64>();

    EXPECT_EQ(0u, row->At(0));
    EXPECT_EQ(1u, (*row)[1]);
    EXPECT_EQ(2u, (*row)[2]);
}

TEST_P(RoundtripCase, ArrayTArrayTUint64) {
    const std::vector<std::vector<uint64_t>> row_values = {
        {1, 2, 3},
        {4, 5, 6},
        {7, 8, 9, 10}
    };

    auto array = std::make_shared<ColumnArrayT<ColumnArrayT<ColumnUInt64>>>();
    array->Append(row_values);

    auto result_typed = ColumnArrayT<ColumnArrayT<ColumnUInt64>>::Wrap(RoundtripColumnValues(*client_, array));
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}

TEST_P(RoundtripCase, ArrayTArrayTArrayTUint64) {
    using ColumnType = ColumnArrayT<ColumnArrayT<ColumnArrayT<ColumnUInt64>>>;
    const std::vector<std::vector<std::vector<uint64_t>>> row_values = {
        {{1, 2, 3}, {3, 2, 1}},
        {{4, 5, 6}, {6, 5, 4}},
        {{7, 8, 9, 10}, {}},
        {{}, {10, 9, 8, 7}}
    };

    auto array = std::make_shared<ColumnType>();
    array->Append(row_values);

    auto result_typed = ColumnType::Wrap(RoundtripColumnValues(*client_, array));
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}


TEST_P(RoundtripCase, ArrayTFixedString) {
    auto array = std::make_shared<ColumnArrayT<ColumnFixedString>>(6);
    array->Append({"hello", "world"});

    auto result_typed = ColumnArrayT<ColumnFixedString>::Wrap(RoundtripColumnValues(*client_, array));
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}

TEST_P(RoundtripCase, ArrayTString) {
    auto array = std::make_shared<ColumnArrayT<ColumnString>>();
    array->Append({"hello", "world"});

    auto result_typed = ColumnArrayT<ColumnString>::Wrap(RoundtripColumnValues(*client_, array));
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}

TEST_P(RoundtripCase, MapTUint64String) {
    using Map = ColumnMapT<ColumnUInt64, ColumnString>;
    auto map = std::make_shared<Map>(std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>());

    std::map<uint64_t, std::string> row;
    row[1] = "hello";
    row[2] = "world";
    map->Append(row);

    auto result_typed = Map::Wrap(RoundtripColumnValues(*client_, map));
    EXPECT_TRUE(CompareRecursive(*map, *result_typed));
}

TEST_P(RoundtripCase, MapUUID_Tuple_String_Array_Uint64) {
    using Tuple = ColumnTupleT<ColumnString, ColumnArrayT<ColumnUInt64>>;
    using Map = ColumnMapT<ColumnUUID, Tuple>;
    auto map = std::make_shared<Map>(std::make_shared<ColumnUUID>(), std::make_shared<Tuple>(
       std::make_tuple(std::make_shared<ColumnString>(), std::make_shared<ColumnArrayT<ColumnUInt64>>())));


    std::map<UUID, std::tuple<std::string, std::vector<uint64_t>>> row;
    row[UUID{1, 1}] = std::make_tuple("hello", std::vector<uint64_t>{1, 2, 3}) ;
    row[UUID{2, 2}] = std::make_tuple("world", std::vector<uint64_t>{4, 5, 6}) ;
    map->Append(row);

    auto result_typed = Map::Wrap(RoundtripColumnValues(*client_, map));
    EXPECT_TRUE(CompareRecursive(*map, *result_typed));
}

TEST_P(RoundtripCase, Point) {
    if (GetSettingValue("allow_experimental_geo_types") != "1") {
       GTEST_SKIP() << "Test is skipped because experimental geo types are not allowed. Set setting allow_experimental_geo_types = 1 in order to allow it." << std::endl;
    }

    auto col = std::make_shared<ColumnPoint>();
    col->Append({1.0, 2.0});
    col->Append({0.1, 0.2});

    auto result_typed = RoundtripColumnValues(*client_, col)->AsStrict<ColumnPoint>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, Ring) {
    if (GetSettingValue("allow_experimental_geo_types") != "1") {
       GTEST_SKIP() << "Test is skipped because experimental geo types are not allowed. Set setting allow_experimental_geo_types = 1 in order to allow it." << std::endl;
    }

    auto col = std::make_shared<ColumnRing>();
    {
        std::vector<ColumnPoint::ValueType> ring{{1.0, 2.0}, {3.0, 4.0}};
        col->Append(ring);
    }
    {
        std::vector<ColumnPoint::ValueType> ring{{0.1, 0.2}, {0.3, 0.4}};
        col->Append(ring);
    }

    auto result_typed = RoundtripColumnValues(*client_, col)->AsStrict<ColumnRing>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, Polygon) {
    if (GetSettingValue("allow_experimental_geo_types") != "1") {
       GTEST_SKIP() << "Test is skipped because experimental geo types are not allowed. Set setting allow_experimental_geo_types = 1 in order to allow it." << std::endl;
    }

    auto col = std::make_shared<ColumnPolygon>();
    {
        std::vector<std::vector<ColumnPoint::ValueType>> polygon
            {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
        col->Append(polygon);
    }
    {
        std::vector<std::vector<ColumnPoint::ValueType>> polygon
            {{{0.1, 0.2}, {0.3, 0.4}}, {{0.5, 0.6}, {0.7, 0.8}}};
        col->Append(polygon);
    }

    auto result_typed = RoundtripColumnValues(*client_, col)->AsStrict<ColumnPolygon>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, MultiPolygon) {
    if (GetSettingValue("allow_experimental_geo_types") != "1") {
       GTEST_SKIP() << "Test is skipped because experimental geo types are not allowed. Set setting allow_experimental_geo_types = 1 in order to allow it." << std::endl;
    }

    auto col = std::make_shared<ColumnMultiPolygon>();
    {
        std::vector<std::vector<std::vector<ColumnPoint::ValueType>>> multi_polygon
            {{{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}},
             {{{1.1, 2.2}, {3.3, 4.4}}, {{5.5, 6.6}, {7.7, 8.8}}}};
        col->Append(multi_polygon);
    }
    {
        std::vector<std::vector<std::vector<ColumnPoint::ValueType>>> multi_polygon
            {{{{0.1, 0.2}, {0.3, 0.4}}, {{0.5, 0.6}, {0.7, 0.8}}},
             {{{1.1, 1.2}, {1.3, 1.4}}, {{1.5, 1.6}, {1.7, 1.8}}}};
        col->Append(multi_polygon);
    }

    auto result_typed = RoundtripColumnValues(*client_, col)->AsStrict<ColumnMultiPolygon>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, LowCardinalityTString) {
    using TestColumn = ColumnLowCardinalityT<ColumnString>;
    auto col = std::make_shared<TestColumn>();

    col->Append("abc");
    col->Append("def");
    col->Append("abc");
    col->Append("abc");

    auto result_typed = RoundtripColumnValues(*client_, col)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, LowCardinalityTNullableString) {
    using TestColumn = ColumnLowCardinalityT<ColumnNullableT<ColumnString>>;
    auto col = std::make_shared<TestColumn>();

    col->Append("abc");
    col->Append("def");
    col->Append("abc");
    col->Append(std::nullopt);
    col->Append("abc");
    col->Append(std::nullopt);
    col->Append(std::nullopt);
    col->Append("foobar");

    auto result_typed = RoundtripColumnValues(*client_, col)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, ArrayTNullableString) {
    using TestColumn = ColumnArrayT<ColumnNullableT<ColumnString>>;
    auto col = std::make_shared<TestColumn>();

    col->Append({std::nullopt, std::nullopt, std::nullopt});
    col->Append(std::vector<std::optional<std::string>>{"abc", std::nullopt});

    auto result_typed = RoundtripColumnValues(*client_, col)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, TupleTNullableString) {
    using TestColumn = ColumnTupleT<ColumnNullableT<ColumnString>>;
    auto col = std::make_shared<TestColumn>(std::make_tuple(std::make_shared<ColumnNullableT<ColumnString>>()));

    col->Append(std::make_tuple(std::nullopt));
    col->Append(std::make_tuple("abc"));

    auto result_typed = RoundtripColumnValues(*client_, col)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, Map_TString_TNullableString) {
    using Key =  ColumnString;
    using Value = ColumnNullableT<ColumnString>;
    using TestColumn = ColumnMapT<Key, Value>;
    auto col = std::make_shared<TestColumn>(std::make_shared<Key>(), std::make_shared<Value>());
    {
        std::map<std::string, std::optional<std::string>> value;
        value["1"] = "one";
        value["2"] = std::nullopt;
        col->Append(value);
    }
    {
        std::map<std::string, std::optional<std::string>> value;
        value["4"] = "one";
        value["2"] = std::nullopt;
        col->Append(value);
    }
    auto result_typed = RoundtripColumnValues(*client_, col)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, Map_LowCardinalityTString_LowCardinalityTNullableString) {
    using Key =  ColumnLowCardinalityT<ColumnString>;
    using Value = ColumnLowCardinalityT<ColumnNullableT<ColumnString>>;
    using TestColumn = ColumnMapT<Key, Value>;

    auto col = std::make_shared<TestColumn>(std::make_shared<Key>(), std::make_shared<Value>());
    {
        std::map<std::string, std::optional<std::string>> value;

        value["1"] = "one";
        value["2"] = std::nullopt;

        col->Append(value);
    }
    {
        std::map<std::string, std::optional<std::string>> value;

        value["4"] = "one";
        value["2"] = std::nullopt;

        col->Append(value);
    }
    auto result_typed = RoundtripColumnValues(*client_, col)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*col, *result_typed));
}

TEST_P(RoundtripCase, RoundtripArrayLowCardinalityTString) {
    using TestColumn = ColumnArrayT<ColumnLowCardinalityT<ColumnString>>;
    auto array = std::make_shared<TestColumn>();

    array->Append(std::vector<std::string>{});
    array->Append(std::vector<std::string>{});

    auto result_typed = RoundtripColumnValues(*client_, array)->As<TestColumn>();
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}

const auto LocalHostEndpoint = ClientOptions()
        .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
        .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
        .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
        .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));

INSTANTIATE_TEST_SUITE_P(
    Roundtrip, RoundtripCase,
    ::testing::Values(
        ClientOptions(LocalHostEndpoint)
            .SetPingBeforeQuery(true)
            .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false),

        ClientOptions(LocalHostEndpoint)
            .SetPingBeforeQuery(false)
            .SetCompressionMethod(CompressionMethod::LZ4)
            .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
    ));
