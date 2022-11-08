#include <clickhouse/base/output.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <gtest/gtest.h>

#include "clickhouse/client.h"
#include "utils.h"

namespace {
using namespace clickhouse;
}

static const auto localHostEndpoint = ClientOptions()
                                          .SetHost(getEnvOrDefault("CLICKHOUSE_HOST", "localhost"))
                                          .SetPort(getEnvOrDefault<size_t>("CLICKHOUSE_PORT", "9000"))
                                          .SetUser(getEnvOrDefault("CLICKHOUSE_USER", "default"))
                                          .SetPassword(getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
                                          .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB", "default"));

template <typename T>
class Generator;

template <typename T>
class GeneratorNumeric {
public:
    static void Generate(std::shared_ptr<T>& col) {
        for (size_t i = 0; i < 1000; ++i) {
            if (i % 10 == 0) {
                col->Append(static_cast<typename T::ValueType>(i));
            } else {
                col->Append(static_cast<typename T::ValueType>(0));
            }
        }
    }
};

template <>
class Generator<ColumnUInt8> : public GeneratorNumeric<ColumnUInt8> {};

template <>
class Generator<ColumnUInt16> : public GeneratorNumeric<ColumnUInt16> {};

template <>
class Generator<ColumnUInt32> : public GeneratorNumeric<ColumnUInt32> {};

template <>
class Generator<ColumnUInt64> : public GeneratorNumeric<ColumnUInt64> {};

template <>
class Generator<ColumnInt8> : public GeneratorNumeric<ColumnInt8> {};

template <>
class Generator<ColumnInt16> : public GeneratorNumeric<ColumnInt16> {};

template <>
class Generator<ColumnInt32> : public GeneratorNumeric<ColumnInt32> {};

template <>
class Generator<ColumnInt64> : public GeneratorNumeric<ColumnInt64> {};

template <>
class Generator<ColumnFloat32> : public GeneratorNumeric<ColumnFloat32> {};

template <>
class Generator<ColumnFloat64> : public GeneratorNumeric<ColumnFloat64> {};

template <>
class Generator<ColumnInt128> : public GeneratorNumeric<ColumnInt128> {};

template <>
class Generator<ColumnDecimal> : public GeneratorNumeric<ColumnDecimal> {};

template <>
class Generator<ColumnDate> : public GeneratorNumeric<ColumnDate> {};

template <>
class Generator<ColumnDateTime> : public GeneratorNumeric<ColumnDateTime> {};

template <>
class Generator<ColumnDateTime64> : public GeneratorNumeric<ColumnDateTime64> {};

template <>
class Generator<ColumnDate32> : public GeneratorNumeric<ColumnDate32> {};

template <typename T>
class GeneratorString {
public:
    static void Generate(std::shared_ptr<T>& col) {
        for (size_t i = 0; i < 1000; ++i) {
            if (i % 10 == 0) {
                col->Append(std::to_string(i));
            } else {
                col->Append("");
            }
        }
    }
};

template <>
class Generator<ColumnString> : public GeneratorString<ColumnString> {};

template <>
class Generator<ColumnFixedString> : public GeneratorString<ColumnFixedString> {};

template <>
class Generator<ColumnIPv4> {
public:
    static void Generate(std::shared_ptr<ColumnIPv4>& col) {
        for (size_t i = 0; i < 1000; ++i) {
            if (i % 10 == 0) {
                col->Append(i);
            } else {
                col->Append(0);
            }
        }
    }
};

template <>
class Generator<ColumnIPv6> {
public:
    static void Generate(std::shared_ptr<ColumnIPv6>& col) {
        unsigned char default_value[16];
        memset(default_value, 0, 16);
        for (size_t i = 0; i < 1000; ++i) {
            if (i % 10 == 0) {
                unsigned char value[16];
                memcpy(value, &i, sizeof(i));
                col->Append(reinterpret_cast<in6_addr*>(value));
            } else {
                col->Append(reinterpret_cast<in6_addr*>(default_value));
            }
        }
    }
};

template <>
class Generator<ColumnUUID> {
public:
    static void Generate(std::shared_ptr<ColumnUUID>& col) {
        UUID default_value{0, 0};
        for (size_t i = 0; i < 1000; ++i) {
            if (i % 10 == 0) {
                UUID value{i, i};
                col->Append(value);
            } else {
                col->Append(default_value);
            }
        }
    }
};

template <typename T>
class GeneratorEnum {
public:
    static void Generate(std::shared_ptr<T>& col) {
        for (size_t i = 0; i < 1000; ++i) {
            if (i % 10 == 0) {
                col->Append(1);
            } else {
                col->Append(0);
            }
        }
    }
};

template <>
class Generator<ColumnEnum8> : public GeneratorEnum<ColumnEnum8> {};

template <>
class Generator<ColumnEnum16> : public GeneratorEnum<ColumnEnum16> {};

template <>
class Generator<ColumnTuple> {
public:
    static void Generate(std::shared_ptr<ColumnTuple>& col) {
        auto int_col = (*col)[0]->template AsStrict<ColumnUInt64>();
        Generator<ColumnUInt64>::Generate(int_col);
        auto string_col = (*col)[1]->template AsStrict<ColumnString>();
        for (size_t i = 0; i < 1000; ++i) {
            string_col->Append(std::to_string(i));
        }
    }
};

template <typename T>
class GenericSparseColumnTest : public testing::Test {
public:
    using ColumnType = std::decay_t<T>;

    static auto MakeColumn() {
        if constexpr (std::is_same_v<ColumnType, ColumnFixedString>) {
            return std::make_shared<ColumnFixedString>(12);
        } else if constexpr (std::is_same_v<ColumnType, ColumnDateTime64>) {
            return std::make_shared<ColumnDateTime64>(3);
        } else if constexpr (std::is_same_v<ColumnType, ColumnDecimal>) {
            return std::make_shared<ColumnDecimal>(10, 5);
        } else if constexpr (std::is_same_v<ColumnType, ColumnEnum8>) {
            return std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"Zero", 0}, {"One", 1}, {"Two", 2}}));
        } else if constexpr (std::is_same_v<ColumnType, ColumnEnum16>) {
            return std::make_shared<ColumnEnum16>(Type::CreateEnum16({{"Zero", 0}, {"One", 1}, {"Two", 2}}));
        } else if constexpr (std::is_same_v<ColumnType, ColumnTuple>) {
            return std::make_shared<ColumnTuple>(
                std::vector<ColumnRef>({std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()}));
        } else {
            return std::make_shared<ColumnType>();
        }
    }

    static auto MakeColumnWithValues() {
        auto col = MakeColumn();
        Generator<ColumnType>::Generate(col);
        return col;
    }

    static auto Compare(const ColumnType& left, const ColumnType& right) {
        if constexpr (std::is_same_v<ColumnType, ColumnTuple>) {
            auto result = CompareRecursive(*left[0]->template AsStrict<ColumnUInt64>(), *right[0]->template AsStrict<ColumnUInt64>());
            if (!result) return result;
            result = CompareRecursive(*left[1]->template AsStrict<ColumnString>(), *right[1]->template AsStrict<ColumnString>());
            return result;
        } else {
            return CompareRecursive(left, right);
        }
    }

    static void SetSerializationKind(ColumnType& column, Serialization::Kind kind) {
        if constexpr (std::is_same_v<ColumnType, ColumnTuple>) {
            column[0]->SetSerializationKind(kind);
        } else {
            column.SetSerializationKind(kind);
        }
    }
};

using ValueColumns =
    ::testing::Types<ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64, ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
                     ColumnFloat32, ColumnFloat64, ColumnString, ColumnFixedString, ColumnDate, ColumnDateTime, ColumnDateTime64,
                     ColumnDate32, ColumnIPv4, ColumnIPv6, ColumnInt128, ColumnDecimal, ColumnUUID, ColumnEnum8, ColumnEnum16, ColumnTuple>;

TYPED_TEST_SUITE(GenericSparseColumnTest, ValueColumns);

TYPED_TEST(GenericSparseColumnTest, LoadAndSave) {
    auto column_A = this->MakeColumnWithValues();

    this->SetSerializationKind(*column_A, Serialization::Kind::SPARSE);

    char buffer[16 * 1024] = {'\0'};
    {
        ArrayOutput output(buffer, sizeof(buffer));
        // Save
        EXPECT_NO_THROW(column_A->Save(&output));
    }

    auto column_B = this->MakeColumn();

    this->SetSerializationKind(*column_B, Serialization::Kind::SPARSE);

    {
        ArrayInput input(buffer, sizeof(buffer));
        // Load
        EXPECT_TRUE(column_B->Load(&input, column_A->Size()));
    }

    EXPECT_TRUE(this->Compare(*column_A, *column_B));
}

TYPED_TEST(GenericSparseColumnTest, SaveSparse) {
    auto column = this->MakeColumnWithValues();

    clickhouse::Client client(localHostEndpoint);

    if (versionNumber(client.GetServerInfo()) < versionNumber(22, 1)) {
        GTEST_SKIP() << "Sparse serialization is available since v22.1.2.2-stable and can't be tested against server: "
                     << client.GetServerInfo();
    }

    const std::string table_name  = "test_clickhouse_cpp_test_ut_sparse_table";
    const std::string column_name = "test_column";
    const auto type_name          = column->GetType().GetName();

    client.Execute("DROP TEMPORARY TABLE IF EXISTS " + table_name + ";");
    client.Execute("CREATE TEMPORARY TABLE IF NOT EXISTS " + table_name + "( " + column_name + " " + type_name + " )");

    Block block;
    block.AppendColumn(column_name, column);

    this->SetSerializationKind(*column, Serialization::Kind::SPARSE);

    client.Insert(table_name, block);

    client.Select("SELECT " + column_name + " FROM " + table_name, [&](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(1U, block.GetColumnCount());
        auto result = block[0]->template AsStrict<typename TestFixture::ColumnType>();
        EXPECT_TRUE(this->Compare(*column, *result));
    });
}

TYPED_TEST(GenericSparseColumnTest, LoadSparse) {
    auto column = this->MakeColumnWithValues();

    clickhouse::Client client(localHostEndpoint);

    if (versionNumber(client.GetServerInfo()) < versionNumber(22, 1)) {
        GTEST_SKIP() << "Sparse serialization is available since v22.1.2.2-stable and can't be tested against server: "
                     << client.GetServerInfo();
    }

    const std::string table_name  = "test_clickhouse_cpp_test_ut_sparse_table";
    const std::string column_name = "test_column";
    const auto type_name          = column->GetType().GetName();

    client.Execute("DROP TABLE IF EXISTS " + table_name + ";");
    try {
        client.Execute("CREATE TABLE IF NOT EXISTS " + table_name + "( id UInt64," + column_name + " " + type_name +
                       " )"
                       " ENGINE = MergeTree ORDER BY id "
                       " SETTINGS index_granularity = 32, "
                       " ratio_of_defaults_for_sparse_serialization = 0.1;");
    } catch (const std::exception& e) {
        std::cerr << "Got error while create table: " << e.what() << std::endl;
        // DB::Exception: clickhouse_cpp_cicd: Cannot execute query in readonly mode
        if (std::string(e.what()).find("Cannot execute query in readonly mode") != std::string::npos) {
            GTEST_SKIP() << "Database in  readonly mode";
        }
        // DB::Exception: clickhouse_cpp_cicd: Not enough privileges. To execute this query it's necessary to have grant CREATE TABLE ON
        // default.test_clickhouse_cpp_test_ut_sparse_table
        if (std::string(e.what()).find("Not enough privileges") != std::string::npos) {
            GTEST_SKIP() << "Not enough privileges";
        }
        throw;
    }

    Block block;
    block.AppendColumn(column_name, column);

    client.Insert(table_name, block);

    client.Select("SELECT " + column_name + " FROM " + table_name, [&](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(1U, block.GetColumnCount());
        auto result = block[0]->template AsStrict<typename TestFixture::ColumnType>();
        if constexpr (std::is_same_v<typename TestFixture::ColumnType, ColumnTuple>) {
            (*column)[0]->SetSerializationKind(Serialization::Kind::DEFAULT);
            ASSERT_EQ((*result)[0]->GetSerialization()->GetKind(), Serialization::Kind::SPARSE);
            ASSERT_EQ((*result)[1]->GetSerialization()->GetKind(), Serialization::Kind::DEFAULT);
        } else {
            ASSERT_EQ(result->GetSerialization()->GetKind(), Serialization::Kind::SPARSE);
        }
        EXPECT_TRUE(this->Compare(*column, *result));
    });

    client.Execute("DROP TABLE IF EXISTS " + table_name + ";");
}
