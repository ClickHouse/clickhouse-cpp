#include <clickhouse/client.h>

#include "readonly_client_test.h"
#include "connection_failed_client_test.h"
#include "utils.h"
#include "roundtrip_column.h"

#include <gtest/gtest.h>

#include <thread>
#include <chrono>

using namespace clickhouse;

// Use value-parameterized tests to run same tests with different client
// options.
class ClientCase : public testing::TestWithParam<ClientOptions> {
protected:
    void SetUp() override {
        client_ = std::make_unique<Client>(GetParam());
    }

    void TearDown() override {
        client_.reset();
    }

    template <typename T>
    std::shared_ptr<T> createTableWithOneColumn(Block & block)
    {
        auto col = std::make_shared<T>();
        const auto type_name = col->GetType().GetName();

        client_->Execute("DROP TEMPORARY TABLE IF EXISTS " + table_name + ";");
        client_->Execute("CREATE TEMPORARY TABLE IF NOT EXISTS " + table_name + "( " + column_name + " " + type_name + " )");

        block.AppendColumn("test_column", col);

        return col;
    }

    std::string getOneColumnSelectQuery() const
    {
        return "SELECT " + column_name + " FROM " + table_name;
    }

    void FlushLogs() {
        try {
            client_->Execute("SYSTEM FLUSH LOGS");
        } catch (const std::exception & e) {
            std::cerr << "Got error while flushing logs: " << e.what() << std::endl;
            const auto wait_for_flush = []() {
                // Insufficient privileges, the only safe way is to wait long enough for system
                // to flush the logs automaticaly. Usually it takes 7.5 seconds, so just in case,
                // wait 3 times that to ensure that all previously executed queries are in the logs now.
                const auto wait_duration = std::chrono::seconds(23);
                std::cerr << "Now we wait " << wait_duration << "..." << std::endl;
                std::this_thread::sleep_for(wait_duration);
            };
            // DB::Exception: clickhouse_cpp_cicd: Not enough privileges. To execute this query it's necessary to have grant SYSTEM FLUSH LOGS ON
            if (std::string(e.what()).find("To execute this query it's necessary to have grant SYSTEM FLUSH LOGS ON") != std::string::npos) {
                wait_for_flush();
            }
            // DB::Exception: clickhouse_cpp_cicd: Cannot execute query in readonly mode
            if (std::string(e.what()).find("Cannot execute query in readonly mode") != std::string::npos) {
                wait_for_flush();
            }
        }
    }

    std::unique_ptr<Client> client_;
    const std::string table_name = "test_clickhouse_cpp_test_ut_table";
    const std::string column_name = "test_column";
};

TEST_P(ClientCase, Array) {
    Block b;

    /// Create a table.
    client_->Execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_array (arr Array(UInt64)) ");

    /// Insert some values.
    {
        auto arr = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        arr->AppendAsColumn(id);

        id->Append(3);
        arr->AppendAsColumn(id);

        id->Append(7);
        arr->AppendAsColumn(id);

        id->Append(9);
        arr->AppendAsColumn(id);

        b.AppendColumn("arr", arr);
        client_->Insert("test_clickhouse_cpp_array", b);
    }

    const uint64_t ARR_SIZE[] = { 1, 2, 3, 4 };
    const uint64_t VALUE[] = { 1, 3, 7, 9 };
    size_t row = 0;
    client_->Select("SELECT arr FROM test_clickhouse_cpp_array",
            [ARR_SIZE, VALUE, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }
            EXPECT_EQ(1U, block.GetColumnCount());
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                auto col = block[0]->As<ColumnArray>()->GetAsColumn(c);
                EXPECT_EQ(ARR_SIZE[row], col->Size());
                for (size_t i = 0; i < col->Size(); ++i) {
                    EXPECT_EQ(VALUE[i], (*col->As<ColumnUInt64>())[i]);
                }
            }
        }
    );

    EXPECT_EQ(4U, row);
}

TEST_P(ClientCase, Date) {
    Block b;

    /// Create a table.
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_date (d DateTime('UTC')) ");

    auto d = std::make_shared<ColumnDateTime>();
    auto const now = std::time(nullptr);
    d->Append(now);
    b.AppendColumn("d", d);
    client_->Insert("test_clickhouse_cpp_date", b);

    client_->Select("SELECT d FROM test_clickhouse_cpp_date", [&now](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }
            EXPECT_EQ(1U, block.GetRowCount());
            EXPECT_EQ(1U, block.GetColumnCount());
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnDateTime>();
                std::time_t t = col->As<ColumnDateTime>()->At(c);
                EXPECT_EQ(now, t);
                EXPECT_EQ(col->Timezone(), "UTC");
            }
        }
    );
}

TEST_P(ClientCase, LowCardinality) {
    Block block;
    auto lc = createTableWithOneColumn<ColumnLowCardinalityT<ColumnString>>(block);

    const std::vector<std::string> data{{"FooBar", "1", "2", "Foo", "4", "Bar", "Foo", "7", "8", "Foo"}};
    lc->AppendMany(data);

    block.RefreshRowCount();
    client_->Insert(table_name, block);

    size_t total_rows = 0;
    client_->Select(getOneColumnSelectQuery(),
        [&total_rows, &data](const Block& block) {
            total_rows += block.GetRowCount();
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_EQ(1U, block.GetColumnCount());
            if (auto col = block[0]->As<ColumnLowCardinalityT<ColumnString>>()) {
                ASSERT_EQ(data.size(), col->Size());
                for (size_t i = 0; i < col->Size(); ++i) {
                    EXPECT_EQ(data[i], (*col)[i]) << " at index: " << i;
                }
            }
        }
    );

    ASSERT_EQ(total_rows, data.size());
}

TEST_P(ClientCase, LowCardinality_InsertAfterClear) {
    // User can successfully insert values after invoking Clear() on LC column.
    Block block;
    auto lc = createTableWithOneColumn<ColumnLowCardinalityT<ColumnString>>(block);

    // Add some data, but don't care about it much.
    lc->AppendMany(std::vector<std::string_view>{"abc", "def", "123", "abc", "123", "def", "ghi"});
    EXPECT_GT(lc->Size(), 0u);
    EXPECT_GT(lc->GetDictionarySize(), 0u);

    lc->Clear();

    // Now ensure that all data appended after Clear() is inserted properly
    const std::vector<std::string> data{{"FooBar", "1", "2", "Foo", "4", "Bar", "Foo", "7", "8", "Foo"}};
    lc->AppendMany(data);

    block.RefreshRowCount();
    client_->Insert(table_name, block);

    // Now validate that data was properly inserted
    size_t total_rows = 0;
    client_->Select(getOneColumnSelectQuery(),
        [&total_rows, &data](const Block& block) {
            total_rows += block.GetRowCount();
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_EQ(1U, block.GetColumnCount());
            if (auto col = block[0]->As<ColumnLowCardinalityT<ColumnString>>()) {
                ASSERT_EQ(data.size(), col->Size());
                for (size_t i = 0; i < col->Size(); ++i) {
                    EXPECT_EQ(data[i], (*col)[i]) << " at index: " << i;
                }
            }
        }
    );

    ASSERT_EQ(total_rows, data.size());
}

TEST_P(ClientCase, LowCardinalityString_AsString) {
    // Validate that LowCardinality(String) column values can be INSERTed from client as ColumnString
    // and also read on client (enabled by special option) as ColumnString.

    ClientOptions options = GetParam();
    options.SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(true);

    client_ = std::make_unique<Client>(GetParam());
    // client_->Execute("CREATE DATABASE IF NOT EXISTS test_clickhouse_cpp");

    Block block;
    auto col = std::make_shared<ColumnString>();

    client_->Execute("DROP TEMPORARY TABLE IF EXISTS " + table_name + ";");
    client_->Execute("CREATE TEMPORARY TABLE IF NOT EXISTS " + table_name + "( " + column_name + " LowCardinality(String) )");

    block.AppendColumn("test_column", col);

    const std::vector<std::string> data{{"FooBar", "1", "2", "Foo", "4", "Bar", "Foo", "7", "8", "Foo"}};
    for (const auto & v : data)
        col->Append(v);

    block.RefreshRowCount();
    client_->Insert(table_name, block);

    // Now that we can access data via ColumnString instead of ColumnLowCardinalityT<ColumnString>
    size_t total_rows = 0;
    client_->Select(getOneColumnSelectQuery(),
        [&total_rows, &data](const Block& block) {
            total_rows += block.GetRowCount();
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_EQ(1U, block.GetColumnCount());
            if (auto col = block[0]->As<ColumnString>()) {
                ASSERT_EQ(data.size(), col->Size());
                for (size_t i = 0; i < col->Size(); ++i) {
                    EXPECT_EQ(data[i], (*col)[i]) << " at index: " << i;
                }
            }
        }
    );

    ASSERT_EQ(total_rows, data.size());
}

TEST_P(ClientCase, Generic) {
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_client (id UInt64, name String) ");

    const struct {
        uint64_t id;
        std::string name;
    } TEST_DATA[] = {
        { 1, "id" },
        { 3, "foo" },
        { 5, "bar" },
        { 7, "name" },
    };

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        auto name = std::make_shared<ColumnString>();
        for (auto const& td : TEST_DATA) {
            id->Append(td.id);
            name->Append(td.name);
        }

        block.AppendColumn("id"  , id);
        block.AppendColumn("name", name);

        client_->Insert("test_clickhouse_cpp_client", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, name FROM test_clickhouse_cpp_client", [TEST_DATA, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }
            EXPECT_EQ("id", block.GetColumnName(0));
            EXPECT_EQ("name", block.GetColumnName(1));
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                EXPECT_EQ(TEST_DATA[row].id, (*block[0]->As<ColumnUInt64>())[c]);
                EXPECT_EQ(TEST_DATA[row].name, (*block[1]->As<ColumnString>())[c]);
            }
        }
    );
    EXPECT_EQ(sizeof(TEST_DATA)/sizeof(TEST_DATA[0]), row);
}

TEST_P(ClientCase, Nullable) {
    /// Create a table.
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_nullable (id Nullable(UInt64), date Nullable(Date)) ");

    // Round std::time_t to start of date.
    const std::time_t cur_date = std::time(nullptr) / 86400 * 86400;
    const struct {
        uint64_t id;
        uint8_t id_null;
        std::time_t date;
        uint8_t date_null;
    } TEST_DATA[] = {
        { 1, 0, cur_date - 2 * 86400, 0 },
        { 2, 0, cur_date - 1 * 86400, 1 },
        { 3, 1, cur_date + 1 * 86400, 0 },
        { 4, 1, cur_date + 2 * 86400, 1 },
    };

    /// Insert some values.
    {
        Block block;

        {
            auto id = std::make_shared<ColumnUInt64>();
            auto nulls = std::make_shared<ColumnUInt8>();
            for (auto const& td : TEST_DATA) {
                id->Append(td.id);
                nulls->Append(td.id_null);
            }
            block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
        }
        {
            auto date = std::make_shared<ColumnDate>();
            auto nulls = std::make_shared<ColumnUInt8>();
            for (auto const& td : TEST_DATA) {
                date->Append(td.date);
                nulls->Append(td.date_null);
            }
            block.AppendColumn("date", std::make_shared<ColumnNullable>(date, nulls));
        }

        client_->Insert("test_clickhouse_cpp_nullable", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, date FROM test_clickhouse_cpp_nullable",
            [TEST_DATA, &row](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                auto col_id   = block[0]->As<ColumnNullable>();
                auto col_date = block[1]->As<ColumnNullable>();

                EXPECT_EQ(static_cast<bool>(TEST_DATA[row].id_null),
                        col_id->IsNull(c));
                if (!col_id->IsNull(c)) {
                    EXPECT_EQ(TEST_DATA[row].id,
                            col_id->Nested()->As<ColumnUInt64>()->At(c));
                }

                EXPECT_EQ(static_cast<bool>(TEST_DATA[row].date_null),
                        col_date->IsNull(c));
                if (!col_date->IsNull(c)) {
                    // Because date column type is Date instead of
                    // DateTime, round to start second of date for test.
                    EXPECT_EQ(TEST_DATA[row].date,
                            col_date->Nested()->As<ColumnDate>()->At(c));
                }
            }
        }
    );

    EXPECT_EQ(sizeof(TEST_DATA) / sizeof(TEST_DATA[0]), row);
}

TEST_P(ClientCase, Numbers) {
    size_t num = 0;

    client_->Select("SELECT number, number FROM system.numbers LIMIT 100000", [&num](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }
            auto col = block[0]->As<ColumnUInt64>();

            for (size_t i = 0; i < col->Size(); ++i, ++num) {
                EXPECT_EQ(num, col->At(i));
            }
        }
    );
    EXPECT_EQ(100000U, num);
}

TEST_P(ClientCase, SimpleAggregateFunction) {
    const auto & server_info = client_->GetServerInfo();
    if (versionNumber(server_info) < versionNumber(19, 9)) {
        GTEST_SKIP() << "Test is skipped since server '" << server_info << "' does not support SimpleAggregateFunction" << std::endl;
    }

    client_->Execute("DROP TEMPORARY TABLE IF EXISTS test_clickhouse_cpp_SimpleAggregateFunction");
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_SimpleAggregateFunction (saf SimpleAggregateFunction(sum, UInt64))");

    constexpr size_t EXPECTED_ROWS = 10;
    client_->Execute("INSERT INTO test_clickhouse_cpp_SimpleAggregateFunction (saf) SELECT number FROM system.numbers LIMIT 10");

    size_t total_rows = 0;
    client_->Select("Select * FROM test_clickhouse_cpp_SimpleAggregateFunction", [&total_rows](const Block & block) {
        if (block.GetRowCount() == 0)
            return;

        total_rows += block.GetRowCount();
        auto col = block[0]->As<ColumnUInt64>();
        ASSERT_NE(nullptr, col);

        for (size_t r = 0; r < col->Size(); ++r) {
            EXPECT_EQ(r, col->At(r));
        }

        EXPECT_EQ(total_rows, col->Size());
    });

    EXPECT_EQ(EXPECTED_ROWS, total_rows);
}

TEST_P(ClientCase, Cancellable) {
    /// Create a table.
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_cancel (x UInt64) ");

    /// Insert a few blocks. In order to make cancel have effect, we have to
    /// insert a relative larger amount of data.
    const int kBlock = 10;
    const int kRowEachBlock = 1000000;
    for (unsigned j = 0; j < kBlock; j++) {
        Block b;

        auto x = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < kRowEachBlock; i++) {
            x->Append(i);
        }

        b.AppendColumn("x", x);
        client_->Insert("test_clickhouse_cpp_cancel", b);
    }

    /// Send a query which is canceled after receiving the first blockr.
    int row_cnt = 0;
    EXPECT_NO_THROW(
        client_->SelectCancelable("SELECT * FROM test_clickhouse_cpp_cancel",
            [&row_cnt](const Block& block)
            {
                row_cnt += block.GetRowCount();
                return false;
            }
        );
    );
    /// It's easier to get query cancelled for compress enabled client.
    EXPECT_LE(row_cnt, kBlock * kRowEachBlock);
}

TEST_P(ClientCase, Exception) {
    /// Create a table.
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_exceptions (id UInt64, name String) ");

    /// Expect failing on table creation.
    EXPECT_THROW(
        client_->Execute(
            "CREATE TEMPORARY TABLE test_clickhouse_cpp_exceptions (id UInt64, name String) "),
        ServerException);
}

TEST_P(ClientCase, Enum) {
    /// Create a table.
    client_->Execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_cpp_enums (id UInt64, e Enum8('One' = 1, 'Two' = 2)) ");

    const struct {
        uint64_t id;
        int8_t eval;
        std::string ename;
    } TEST_DATA[] = {
        { 1, 1, "One" },
        { 2, 2, "Two" },
        { 3, 2, "Two" },
        { 4, 1, "One", },
    };

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        auto e = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"One", 1}, {"Two", 2}}));

        int i = 0;
        for (auto const& td : TEST_DATA) {
            id->Append(td.id);
            if (++i % 2) {
                e->Append(td.eval);
            } else {
                e->Append(td.ename);
            }
        }

        block.AppendColumn("id", id);
        block.AppendColumn("e", e);

        client_->Insert("test_clickhouse_cpp_enums", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, e FROM test_clickhouse_cpp_enums", [&row, TEST_DATA](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            EXPECT_EQ("id", block.GetColumnName(0));
            EXPECT_EQ("e", block.GetColumnName(1));
            for (size_t i = 0; i < block.GetRowCount(); ++i, ++row) {
                EXPECT_EQ(TEST_DATA[row].id, (*block[0]->As<ColumnUInt64>())[i]);
                EXPECT_EQ(TEST_DATA[row].eval, (*block[1]->As<ColumnEnum8>()).At(i));
                EXPECT_EQ(TEST_DATA[row].ename, (*block[1]->As<ColumnEnum8>()).NameAt(i));
            }
        }
    );
    EXPECT_EQ(sizeof(TEST_DATA)/sizeof(TEST_DATA[0]), row);
}

TEST_P(ClientCase, Decimal) {
    client_->Execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS "
        "test_clickhouse_cpp_decimal (id UInt64, d1 Decimal(9, 4), d2 Decimal(18, 9), d3 Decimal(38, 19), "
        "                         d4 Decimal32(4), d5 Decimal64(9), d6 Decimal128(19)) ");

    {
        Block b;

        auto id = std::make_shared<ColumnUInt64>();
        auto d1 = std::make_shared<ColumnDecimal>(9, 4);
        auto d2 = std::make_shared<ColumnDecimal>(18, 9);
        auto d3 = std::make_shared<ColumnDecimal>(38, 19);
        auto d4 = std::make_shared<ColumnDecimal>(9, 4);
        auto d5 = std::make_shared<ColumnDecimal>(18, 9);
        auto d6 = std::make_shared<ColumnDecimal>(38, 19);

        EXPECT_THROW(
            d1->Append("1234567890123456789012345678901234567890"),
            std::runtime_error
        );
        EXPECT_THROW(
            d1->Append("123456789012345678901234567890123456.7890"),
            std::runtime_error
        );
        EXPECT_THROW(
            d1->Append("-1234567890123456789012345678901234567890"),
            std::runtime_error
        );
        EXPECT_THROW(
            d1->Append("12345678901234567890123456789012345678a"),
            std::runtime_error
        );
        EXPECT_THROW(
            d1->Append("12345678901234567890123456789012345678-"),
            std::runtime_error
        );
        EXPECT_THROW(
            d1->Append("1234.12.1234"),
            std::runtime_error
        );

        id->Append(1);
        d1->Append(123456789);
        d2->Append(123456789012345678);
        d3->Append(1234567890123456789);
        d4->Append(123456789);
        d5->Append(123456789012345678);
        d6->Append(1234567890123456789);

        id->Append(2);
        d1->Append(999999999);
        d2->Append(999999999999999999);
        d3->Append(999999999999999999);
        d4->Append(999999999);
        d5->Append(999999999999999999);
        d6->Append(999999999999999999);

        id->Append(3);
        d1->Append(-999999999);
        d2->Append(-999999999999999999);
        d3->Append(-999999999999999999);
        d4->Append(-999999999);
        d5->Append(-999999999999999999);
        d6->Append(-999999999999999999);

        // Check strings with decimal point
        id->Append(4);
        d1->Append("12345.6789");
        d2->Append("123456789.012345678");
        d3->Append("1234567890123456789.0123456789012345678");
        d4->Append("12345.6789");
        d5->Append("123456789.012345678");
        d6->Append("1234567890123456789.0123456789012345678");

        // Check strings with minus sign and without decimal point
        id->Append(5);
        d1->Append("-12345.6789");
        d2->Append("-123456789012345678");
        d3->Append("-12345678901234567890123456789012345678");
        d4->Append("-12345.6789");
        d5->Append("-123456789012345678");
        d6->Append("-12345678901234567890123456789012345678");

        id->Append(6);
        d1->Append("12345.678");
        d2->Append("123456789.0123456789");
        d3->Append("1234567890123456789.0123456789012345678");
        d4->Append("12345.6789");
        d5->Append("123456789.012345678");
        d6->Append("1234567890123456789.0123456789012345678");

        b.AppendColumn("id", id);
        b.AppendColumn("d1", d1);
        b.AppendColumn("d2", d2);
        b.AppendColumn("d3", d3);
        b.AppendColumn("d4", d4);
        b.AppendColumn("d5", d5);
        b.AppendColumn("d6", d6);

        client_->Insert("test_clickhouse_cpp_decimal", b);
    }

    client_->Select("SELECT id, d1, d2, d3, d4, d5, d6 FROM test_clickhouse_cpp_decimal ORDER BY id", [](const Block& b) {
        if (b.GetRowCount() == 0) {
            return;
        }

        ASSERT_EQ(6u, b.GetRowCount());

        auto int128_to_string = [](Int128 value) {
            std::string result;
            const bool sign = value >= 0;

            if (!sign) {
                value = -value;
            }

            while (value) {
                result += static_cast<char>(value % 10) + '0';
                value /= 10;
            }

            if (result.empty()) {
                result = "0";
            } else if (!sign) {
                result.push_back('-');
            }

            std::reverse(result.begin(), result.end());

            return result;
        };

        auto decimal = [&b](size_t column, size_t row) {
            return b[column]->As<ColumnDecimal>()->At(row);
        };

        EXPECT_EQ(1u, b[0]->As<ColumnUInt64>()->At(0));
        EXPECT_EQ("123456789", int128_to_string(decimal(1, 0)));
        EXPECT_EQ("123456789012345678", int128_to_string(decimal(2, 0)));
        EXPECT_EQ("1234567890123456789", int128_to_string(decimal(3, 0)));
        EXPECT_EQ("123456789", int128_to_string(decimal(4, 0)));
        EXPECT_EQ("123456789012345678", int128_to_string(decimal(5, 0)));
        EXPECT_EQ("1234567890123456789", int128_to_string(decimal(6, 0)));

        EXPECT_EQ(2u, b[0]->As<ColumnUInt64>()->At(1));
        EXPECT_EQ("999999999", int128_to_string(decimal(1, 1)));
        EXPECT_EQ("999999999999999999", int128_to_string(decimal(2, 1)));
        EXPECT_EQ("999999999999999999", int128_to_string(decimal(3, 1)));
        EXPECT_EQ("999999999", int128_to_string(decimal(4, 1)));
        EXPECT_EQ("999999999999999999", int128_to_string(decimal(5, 1)));
        EXPECT_EQ("999999999999999999", int128_to_string(decimal(6, 1)));

        EXPECT_EQ(3u, b[0]->As<ColumnUInt64>()->At(2));
        EXPECT_EQ("-999999999", int128_to_string(decimal(1, 2)));
        EXPECT_EQ("-999999999999999999", int128_to_string(decimal(2, 2)));
        EXPECT_EQ("-999999999999999999", int128_to_string(decimal(3, 2)));
        EXPECT_EQ("-999999999", int128_to_string(decimal(4, 2)));
        EXPECT_EQ("-999999999999999999", int128_to_string(decimal(5, 2)));
        EXPECT_EQ("-999999999999999999", int128_to_string(decimal(6, 2)));

        EXPECT_EQ(4u, b[0]->As<ColumnUInt64>()->At(3));
        EXPECT_EQ("123456789", int128_to_string(decimal(1, 3)));
        EXPECT_EQ("123456789012345678", int128_to_string(decimal(2, 3)));
        EXPECT_EQ("12345678901234567890123456789012345678", int128_to_string(decimal(3, 3)));
        EXPECT_EQ("123456789", int128_to_string(decimal(4, 3)));
        EXPECT_EQ("123456789012345678", int128_to_string(decimal(5, 3)));
        EXPECT_EQ("12345678901234567890123456789012345678", int128_to_string(decimal(6, 3)));

        EXPECT_EQ(5u, b[0]->As<ColumnUInt64>()->At(4));
        EXPECT_EQ("-123456789", int128_to_string(decimal(1, 4)));
        EXPECT_EQ("-123456789012345678", int128_to_string(decimal(2, 4)));
        EXPECT_EQ("-12345678901234567890123456789012345678", int128_to_string(decimal(3, 4)));
        EXPECT_EQ("-123456789", int128_to_string(decimal(4, 4)));
        EXPECT_EQ("-123456789012345678", int128_to_string(decimal(5, 4)));
        EXPECT_EQ("-12345678901234567890123456789012345678", int128_to_string(decimal(6, 4)));

        EXPECT_EQ(6u, b[0]->As<ColumnUInt64>()->At(5));
        EXPECT_EQ("123456780", int128_to_string(decimal(1, 5)));
        EXPECT_EQ("123456789012345678", int128_to_string(decimal(2, 5)));
        EXPECT_EQ("12345678901234567890123456789012345678", int128_to_string(decimal(3, 5)));
        EXPECT_EQ("123456789", int128_to_string(decimal(4, 5)));
        EXPECT_EQ("123456789012345678", int128_to_string(decimal(5, 5)));
        EXPECT_EQ("12345678901234567890123456789012345678", int128_to_string(decimal(6, 5)));
    });
}

// Test special chars in names
TEST_P(ClientCase, ColEscapeNameTest) {
    client_->Execute(R"sql(DROP TEMPORARY TABLE IF EXISTS "test_clickhouse_cpp_col_escape_""name_test";)sql");

    client_->Execute(R"sql(CREATE TEMPORARY TABLE IF NOT EXISTS "test_clickhouse_cpp_col_escape_""name_test" ("test space" UInt64, "test "" quote" UInt64, "test ""`'[]&_\ all" UInt64))sql");

    auto col1 = std::make_shared<ColumnUInt64>();
    col1->Append(1);
    col1->Append(2);
    auto col2 = std::make_shared<ColumnUInt64>();
    col2->Append(4);
    col2->Append(8);
    auto col3 = std::make_shared<ColumnUInt64>();
    col3->Append(16);
    col3->Append(32);

    static const std::string column_names[] = {
        "test space",
        R"sql(test " quote)sql",
        R"sql(test "`'[]&_\ all)sql"
    };
    static const auto columns_count = sizeof(column_names)/sizeof(column_names[0]);

    Block block;
    block.AppendColumn(column_names[0], col1);
    block.AppendColumn(column_names[1], col2);
    block.AppendColumn(column_names[2], col3);

    client_->Insert(R"sql("test_clickhouse_cpp_col_escape_""name_test")sql", block);
    client_->Select(R"sql(SELECT * FROM "test_clickhouse_cpp_col_escape_""name_test")sql", [] (const Block& sblock)
    {
        int row = sblock.GetRowCount();
        if (row <= 0) {return;}
        ASSERT_EQ(columns_count, sblock.GetColumnCount());
        for (size_t i = 0; i < columns_count; ++i) {
            EXPECT_EQ(column_names[i], sblock.GetColumnName(i));
        }

        EXPECT_EQ(row, 2);
        EXPECT_EQ(sblock[0]->As<ColumnUInt64>()->At(0), 1u);
        EXPECT_EQ(sblock[0]->As<ColumnUInt64>()->At(1), 2u);
        EXPECT_EQ(sblock[1]->As<ColumnUInt64>()->At(0), 4u);
        EXPECT_EQ(sblock[1]->As<ColumnUInt64>()->At(1), 8u);
        EXPECT_EQ(sblock[2]->As<ColumnUInt64>()->At(0), 16u);
        EXPECT_EQ(sblock[2]->As<ColumnUInt64>()->At(1), 32u);
    });
}

// Test roundtrip of DateTime64 values
TEST_P(ClientCase, DateTime64) {
    const auto & server_info = client_->GetServerInfo();
    if (versionNumber(server_info) < versionNumber(20, 1)) {
        GTEST_SKIP() << "Test is skipped since server '" << server_info << "' does not support DateTime64" << std::endl;
    }

    Block block;
    client_->Execute("DROP TEMPORARY TABLE IF EXISTS test_clickhouse_cpp_datetime64;");

    client_->Execute("CREATE TEMPORARY TABLE IF NOT EXISTS "
            "test_clickhouse_cpp_datetime64 (dt DateTime64(6)) ");

    auto col_dt64 = std::make_shared<ColumnDateTime64>(6);
    block.AppendColumn("dt", col_dt64);

    // Empty INSERT and SELECT
    client_->Insert("test_clickhouse_cpp_datetime64", block);
    client_->Select("SELECT dt FROM test_clickhouse_cpp_datetime64",
        [](const Block& block) {
            ASSERT_EQ(0U, block.GetRowCount());
        }
    );

    const std::vector<Int64> data{
        -1'234'567'890'123'456'7ll, // approx year 1578
        -1'234'567'890'123ll,       // 1969-12-17T17:03:52.890123
        -1'234'567ll,               // 1969-12-31T23:59:58.234567
        0,                          // epoch
        1'234'567ll,                // 1970-01-01T00:00:01.234567
        1'234'567'890'123ll,        // 1970-01-15T06:56:07.890123
        1'234'567'890'123'456'7ll   // 2361-03-21T19:15:01.234567
    };
    for (const auto & d : data) {
        col_dt64->Append(d);
    }

    block.RefreshRowCount();

    // Non-empty INSERT and SELECT
    client_->Insert("test_clickhouse_cpp_datetime64", block);

    size_t total_rows = 0;
    client_->Select("SELECT dt FROM test_clickhouse_cpp_datetime64",
        [&total_rows, &data](const Block& block) {
            total_rows += block.GetRowCount();
            if (block.GetRowCount() == 0) {
                return;
            }

            const auto offset = total_rows - block.GetRowCount();
            ASSERT_EQ(1U, block.GetColumnCount());
            if (auto col = block[0]->As<ColumnDateTime64>()) {
                for (size_t i = 0; i < col->Size(); ++i) {
                    EXPECT_EQ(data[offset + i], col->At(i)) << " at index: " << i;
                }
            }
        }
    );

    ASSERT_EQ(total_rows, data.size());
}

TEST_P(ClientCase, Query_ID) {
    const auto server_info = client_->GetServerInfo();

    std::srand(std::time(nullptr) + reinterpret_cast<int64_t>(&server_info));
    const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string query_id = std::to_string(std::rand()) + "-" + test_info->test_suite_name() + "/" + test_info->name();

    SCOPED_TRACE(query_id);

    const std::string table_name = "test_clickhouse_cpp_query_id_test";
    client_->Execute(Query("CREATE TEMPORARY TABLE IF NOT EXISTS " + table_name + " (a Int64)", query_id));

    {
        Block b;
        b.AppendColumn("a", std::make_shared<ColumnInt64>(std::vector<int64_t>{1, 2, 3}));
        client_->Insert(table_name, query_id, b);
    }

    client_->Select("SELECT 'a', count(*) FROM " + table_name, query_id, [](const Block &) {});
    client_->SelectCancelable("SELECT 'b', count(*) FROM " + table_name, query_id, [](const Block &) { return true; });
    client_->Execute(Query("TRUNCATE TABLE " + table_name, query_id));

    FlushLogs();

    size_t total_count = 0;
    client_->Select("SELECT type, query_kind, query_id, query "
                    " FROM system.query_log "
                    " WHERE type = 'QueryStart' AND query_id == '" + query_id +"'",
        [&total_count](const Block & block) {
            total_count += block.GetRowCount();
//            std::cerr << PrettyPrintBlock{block} << std::endl;
    });

    // We've executed 5 queries with explicit query_id, hence we expect to see 5 entries in logs.
    EXPECT_EQ(5u, total_count);
}

// Spontaneosly fails on INSERTint data.
TEST_P(ClientCase, DISABLED_ArrayArrayUInt64) {
    // Based on https://github.com/ClickHouse/clickhouse-cpp/issues/43
    std::cerr << "Connected to: " << client_->GetServerInfo() << std::endl;
    std::cerr << "DROPPING TABLE" << std::endl;
    client_->Execute("DROP TEMPORARY TABLE IF EXISTS multiarray");

    std::cerr << "CREATING TABLE" << std::endl;
    client_->Execute(Query(R"sql(CREATE TEMPORARY TABLE IF NOT EXISTS multiarray
    (
        `arr` Array(Array(UInt64))
    );
)sql"));

    std::cerr << "INSERTING VALUES" << std::endl;
    client_->Execute(Query(R"sql(INSERT INTO multiarray VALUES ([[0,1,2,3,4,5], [100, 200], [10,20, 50, 70]]), ([[456, 789], [1011, 1213], [], [14]]), ([[]]);)sql"));
    std::cerr << "INSERTED" << std::endl;

    auto result = std::make_shared<ColumnArray>(std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>()));
    ASSERT_EQ(0u, result->Size());

    std::cerr << "SELECTING VALUES" << std::endl;
    client_->Select("SELECT arr FROM multiarray", [&result](const Block& block) {
        std::cerr << "GOT BLOCK: " << block.GetRowCount() << std::endl;
        if (block.GetRowCount() == 0)
            return;

        result->Append(block[0]);
    });

    std::cerr << "DONE SELECTING VALUES" << std::endl;
    client_.reset();

    ASSERT_EQ(3u, result->Size());
    {
        // ([[0,1,2,3,4,5], [100, 200], [10,20, 50, 70]])
        const std::vector<std::vector<uint64_t>> expected_vals = {
            {0, 1, 2, 3, 4, 5},
            {100, 200},
            {10, 20, 50, 70}
        };

        auto row = result->GetAsColumnTyped<ColumnArray>(0);
        ASSERT_EQ(3u, row->Size());
        EXPECT_TRUE(CompareRecursive(expected_vals[0], *row->GetAsColumnTyped<ColumnUInt64>(0)));
        EXPECT_TRUE(CompareRecursive(expected_vals[1], *row->GetAsColumnTyped<ColumnUInt64>(1)));
        EXPECT_TRUE(CompareRecursive(expected_vals[2], *row->GetAsColumnTyped<ColumnUInt64>(2)));
    }

    {
        // ([[456, 789], [1011, 1213], [], [14]])
        const std::vector<std::vector<uint64_t>> expected_vals = {
            {456, 789},
            {1011, 1213},
            {},
            {14}
        };

        auto row = result->GetAsColumnTyped<ColumnArray>(1);
        ASSERT_EQ(4u, row->Size());
        EXPECT_TRUE(CompareRecursive(expected_vals[0], *row->GetAsColumnTyped<ColumnUInt64>(0)));
        EXPECT_TRUE(CompareRecursive(expected_vals[1], *row->GetAsColumnTyped<ColumnUInt64>(1)));
        EXPECT_TRUE(CompareRecursive(expected_vals[2], *row->GetAsColumnTyped<ColumnUInt64>(2)));
        EXPECT_TRUE(CompareRecursive(expected_vals[3], *row->GetAsColumnTyped<ColumnUInt64>(3)));
    }

    {
        // ([[]])
        auto row = result->GetAsColumnTyped<ColumnArray>(2);
        ASSERT_EQ(1u, row->Size());
        EXPECT_TRUE(CompareRecursive(std::vector<uint64_t>{}, *row->GetAsColumnTyped<ColumnUInt64>(0)));
    }
}

TEST_P(ClientCase, RoundtripArrayTUint64) {
    auto array = std::make_shared<ColumnArrayT<ColumnUInt64>>();
    array->Append({0, 1, 2});

    auto result = RoundtripColumnValues(*client_, array)->AsStrict<ColumnArray>();
    auto row = result->GetAsColumn(0)->As<ColumnUInt64>();

    EXPECT_EQ(0u, row->At(0));
    EXPECT_EQ(1u, (*row)[1]);
    EXPECT_EQ(2u, (*row)[2]);
}

TEST_P(ClientCase, RoundtripArrayTArrayTUint64) {
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

TEST_P(ClientCase, RoundtripArrayTArrayTArrayTUint64) {
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


TEST_P(ClientCase, RoundtripArrayTFixedString) {
    auto array = std::make_shared<ColumnArrayT<ColumnFixedString>>(6);
    array->Append({"hello", "world"});

    auto result_typed = ColumnArrayT<ColumnFixedString>::Wrap(RoundtripColumnValues(*client_, array));
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}

TEST_P(ClientCase, RoundtripArrayTString) {
    auto array = std::make_shared<ColumnArrayT<ColumnString>>();
    array->Append({"hello", "world"});

    auto result_typed = ColumnArrayT<ColumnString>::Wrap(RoundtripColumnValues(*client_, array));
    EXPECT_TRUE(CompareRecursive(*array, *result_typed));
}

TEST_P(ClientCase, OnProgress) {
    Block block;
    createTableWithOneColumn<ColumnString>(block);

    std::optional<Progress> received_progress;
    Query query("INSERT INTO " + table_name + " (*) VALUES (\'Foo\'), (\'Bar\')" );
    query.OnProgress([&](const Progress& progress) {
            received_progress = progress;
        });
    client_->Execute(query);

    ASSERT_TRUE(received_progress.has_value());

    EXPECT_GE(received_progress->rows, 0u);
    EXPECT_LE(received_progress->rows, 2u);

    EXPECT_GE(received_progress->bytes, 0u);
    EXPECT_LE(received_progress->bytes, 10000u);

    EXPECT_GE(received_progress->total_rows, 0u);
    EXPECT_LE(received_progress->total_rows, 2u);

    EXPECT_GE(received_progress->written_rows, 0u);
    EXPECT_LE(received_progress->written_rows, 2u);

    EXPECT_GE(received_progress->written_bytes, 0u);
    EXPECT_LE(received_progress->written_bytes, 10000u);
}

TEST_P(ClientCase, QuerySettings) {
    client_->Execute("DROP TEMPORARY TABLE IF EXISTS test_clickhouse_query_settings_table_1;");
    client_->Execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_query_settings_table_1 ( id  Int64 )");

    client_->Execute("DROP TEMPORARY TABLE IF EXISTS test_clickhouse_query_settings_table_2;");
    client_->Execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_clickhouse_query_settings_table_2 ( id  Int64, value Int64 )");

    client_->Execute("INSERT INTO test_clickhouse_query_settings_table_1 (*) VALUES (1)");

    Query query("SELECT value "
                "FROM test_clickhouse_query_settings_table_1 "
                "LEFT OUTER JOIN test_clickhouse_query_settings_table_2 "
                "ON test_clickhouse_query_settings_table_1.id = test_clickhouse_query_settings_table_2.id");


    bool checked = false;

    query.SetSetting("join_use_nulls", {"1"});

    query.OnData(
        [&](const Block& block) {
            if (block.GetRowCount() == 0)
                return;
            ASSERT_EQ(1U, block.GetColumnCount());
            ASSERT_EQ(1U, block.GetRowCount());
            ASSERT_TRUE(block[0]->GetType().IsEqual(Type::CreateNullable(Type::CreateSimple<int64_t>())));
            auto cl = block[0]->As<ColumnNullable>();
            EXPECT_TRUE(cl->IsNull(0));
            checked = true;
        });
    client_->Execute(query);

    EXPECT_TRUE(checked);

    query.SetSetting("join_use_nulls", {"0"});

    query.OnData(
        [&](const Block& block) {
            if (block.GetRowCount() == 0)
                return;
            ASSERT_EQ(1U, block.GetColumnCount());
            ASSERT_EQ(1U, block.GetRowCount());
            ASSERT_TRUE(block[0]->GetType().IsEqual(Type::CreateSimple<int64_t>()));
            auto cl = block[0]->As<ColumnInt64>();
            EXPECT_EQ(cl->At(0), 0);
            checked = true;
        }
    );
    checked = false;
    client_->Execute(query);

    EXPECT_TRUE(checked);

    query.SetSetting("wrong_setting_name", {"0", QuerySettingsField::IMPORTANT});

    EXPECT_THROW(client_->Execute(query), ServerException);
}

TEST_P(ClientCase, ServerLogs) {

    Block block;
    createTableWithOneColumn<ColumnString>(block);

    size_t received_row_count = 0;
    Query query("INSERT INTO " + table_name + " (*) VALUES (\'Foo\'), (\'Bar\')" );
    query.SetSetting("send_logs_level", {"trace"});
    query.OnServerLog([&](const Block& block) {
        received_row_count += block.GetRowCount();
        return true;
    });
    client_->Execute(query);

    EXPECT_GT(received_row_count, 0U);
}

TEST_P(ClientCase, TracingContext) {
    Block block;
    createTableWithOneColumn<ColumnString>(block);

    Query query("INSERT INTO " + table_name + " (*) VALUES (\'Foo\'), (\'Bar\')" );
    open_telemetry::TracingContext tracing_context;
    std::srand(std::time(0));
    tracing_context.trace_id = {std::rand(), std::rand()};
    query.SetTracingContext(tracing_context);
    client_->Execute(query);

    FlushLogs();

    size_t received_rows = 0;
    client_->Select("SELECT trace_id, toString(trace_id), operation_name "
                   "FROM system.opentelemetry_span_log "
                   "WHERE trace_id = toUUID(\'" + ToString(tracing_context.trace_id) + "\');",
        [&](const Block& block) {
            // std::cerr << PrettyPrintBlock{block} << std::endl;
            received_rows += block.GetRowCount();
    });

    EXPECT_GT(received_rows, 0u);
}

TEST_P(ClientCase, OnProfileEvents) {
    Block block;
    createTableWithOneColumn<ColumnString>(block);

    client_->Execute("INSERT INTO " + table_name + " (*) VALUES (\'Foo\'), (\'Bar\')");
    size_t received_row_count = 0;
    Query query("SELECT * FROM " + table_name);

    query.OnProfileEvents([&](const Block& block) {
        received_row_count += block.GetRowCount();
        return true;
    });
    client_->Execute(query);

    const int DBMS_MIN_REVISION_WITH_INCREMENTAL_PROFILE_EVENTS = 54451;
    if (client_->GetServerInfo().revision >= DBMS_MIN_REVISION_WITH_INCREMENTAL_PROFILE_EVENTS) {
        EXPECT_GT(received_row_count, 0U);
    }
}

const auto LocalHostEndpoint = ClientOptions()
        .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
        .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
        .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
        .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));

INSTANTIATE_TEST_SUITE_P(
    Client, ClientCase,
    ::testing::Values(
        ClientOptions(LocalHostEndpoint)
            .SetPingBeforeQuery(true),
        ClientOptions(LocalHostEndpoint)
            .SetPingBeforeQuery(false)
            .SetCompressionMethod(CompressionMethod::LZ4)
    ));

namespace {
using namespace clickhouse;

const auto QUERIES = std::vector<std::string>{
    "SELECT version()",
    "SELECT fqdn()",
    "SELECT buildId()",
    "SELECT uptime()",
    "SELECT filesystemFree()",
    "SELECT now()"
};
}

INSTANTIATE_TEST_SUITE_P(ClientLocalReadonly, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType{
        ClientOptions(LocalHostEndpoint)
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None),
        QUERIES
    }
));

INSTANTIATE_TEST_SUITE_P(ClientLocalFailed, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType{
        ClientOptions()
            .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
            .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
            .SetUser("non_existing_user_clickhouse_cpp_test")
            .SetPassword("wrongpwd")
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None),
        ExpectingException{"Authentication failed: password is incorrect"}
    }
));
