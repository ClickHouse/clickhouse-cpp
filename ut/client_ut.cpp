#include <clickhouse/client.h>
#include <contrib/gtest/gtest.h>

#include <cmath>

using namespace clickhouse;

namespace clickhouse {
std::ostream & operator<<(std::ostream & ostr, const ServerInfo & server_info) {
    return ostr << server_info.name << "/" << server_info.display_name
                << " ver "
                << server_info.version_major << "."
                << server_info.version_minor << "."
                << server_info.version_patch
                << " (" << server_info.revision << ")";
}
}

namespace {

uint64_t versionNumber(
        uint64_t version_major,
        uint64_t version_minor,
        uint64_t version_patch = 0,
        uint64_t revision = 0) {

    // in this case version_major can be up to 1000
    static auto revision_decimal_places = 8;
    static auto patch_decimal_places = 4;
    static auto minor_decimal_places = 4;

    auto const result = version_major * static_cast<uint64_t>(std::pow(10, minor_decimal_places + patch_decimal_places + revision_decimal_places))
            + version_minor * static_cast<uint64_t>(std::pow(10, patch_decimal_places + revision_decimal_places))
            + version_patch * static_cast<uint64_t>(std::pow(10, revision_decimal_places))
            + revision;

    return result;
}

uint64_t versionNumber(const ServerInfo & server_info) {
    return versionNumber(server_info.version_major, server_info.version_minor, server_info.version_patch, server_info.revision);
}

}

// Use value-parameterized tests to run same tests with different client
// options.
class ClientCase : public testing::TestWithParam<ClientOptions> {
protected:
    void SetUp() override {
        client_ = std::make_unique<Client>(GetParam());
        client_->Execute("CREATE DATABASE IF NOT EXISTS test_clickhouse_cpp");
    }

    void TearDown() override {
        if (client_)
            client_->Execute("DROP DATABASE test_clickhouse_cpp");
    }

    template <typename T>
    std::shared_ptr<T> createTableWithOneColumn(Block & block)
    {
        auto col = std::make_shared<T>();
        const auto type_name = col->GetType().GetName();

        client_->Execute("DROP TABLE IF EXISTS " + table_name + ";");
        client_->Execute("CREATE TABLE IF NOT EXISTS " + table_name + "( " + column_name + " " + type_name + " )"
                "ENGINE = Memory");

        block.AppendColumn("test_column", col);

        return col;
    }

    std::string getOneColumnSelectQuery() const
    {
        return "SELECT " + column_name + " FROM " + table_name;
    }

    std::unique_ptr<Client> client_;
    const std::string table_name = "test_clickhouse_cpp.test_ut_table";
    const std::string column_name = "test_column";
};

TEST_P(ClientCase, Array) {
    Block b;

    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.array (arr Array(UInt64)) "
            "ENGINE = Memory");

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
        client_->Insert("test_clickhouse_cpp.array", b);
    }

    const uint64_t ARR_SIZE[] = { 1, 2, 3, 4 };
    const uint64_t VALUE[] = { 1, 3, 7, 9 };
    size_t row = 0;
    client_->Select("SELECT arr FROM test_clickhouse_cpp.array",
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
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.date (d DateTime) "
            "ENGINE = Memory");

    auto d = std::make_shared<ColumnDateTime>();
    auto const now = std::time(nullptr);
    d->Append(now);
    b.AppendColumn("d", d);
    client_->Insert("test_clickhouse_cpp.date", b);

    client_->Select("SELECT d FROM test_clickhouse_cpp.date", [&now](const Block& block)
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

TEST_P(ClientCase, Generic) {
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.client (id UInt64, name String) "
            "ENGINE = Memory");

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

        client_->Insert("test_clickhouse_cpp.client", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, name FROM test_clickhouse_cpp.client", [TEST_DATA, &row](const Block& block)
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
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.nullable (id Nullable(UInt64), date Nullable(Date)) "
            "ENGINE = Memory");

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

        client_->Insert("test_clickhouse_cpp.nullable", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, date FROM test_clickhouse_cpp.nullable",
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
        std::cout << "Test is skipped since server '" << server_info << "' does not support SimpleAggregateFunction" << std::endl;
        return;
    }

    client_->Execute("DROP TABLE IF EXISTS test_clickhouse_cpp.SimpleAggregateFunction");
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.SimpleAggregateFunction (saf SimpleAggregateFunction(sum, UInt64))"
            "ENGINE = Memory");

    constexpr size_t EXPECTED_ROWS = 10;
    client_->Execute("INSERT INTO test_clickhouse_cpp.SimpleAggregateFunction (saf) SELECT number FROM system.numbers LIMIT 10");

    size_t total_rows = 0;
    client_->Select("Select * FROM test_clickhouse_cpp.SimpleAggregateFunction", [&total_rows](const Block & block) {
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
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.cancel (x UInt64) "
            "ENGINE = Memory");

    /// Insert a few blocks. In order to make cancel have effect, we have to
    /// insert a relative larget amount of data.
    const int kBlock = 10;
    const int kRowEachBlock = 1000000;
    for (unsigned j = 0; j < kBlock; j++) {
        Block b;

        auto x = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < kRowEachBlock; i++) {
            x->Append(i);
        }

        b.AppendColumn("x", x);
        client_->Insert("test_clickhouse_cpp.cancel", b);
    }

    /// Send a query which is canceled after receiving the first blockr.
    int row_cnt = 0;
    EXPECT_NO_THROW(
        client_->SelectCancelable("SELECT * FROM test_clickhouse_cpp.cancel",
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
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.exceptions (id UInt64, name String) "
            "ENGINE = Memory");

    /// Expect failing on table creation.
    EXPECT_THROW(
        client_->Execute(
            "CREATE TABLE test_clickhouse_cpp.exceptions (id UInt64, name String) "
            "ENGINE = Memory"),
        ServerException);
}

TEST_P(ClientCase, Enum) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test_clickhouse_cpp.enums (id UInt64, e Enum8('One' = 1, 'Two' = 2)) "
            "ENGINE = Memory");

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

        client_->Insert("test_clickhouse_cpp.enums", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, e FROM test_clickhouse_cpp.enums", [&row, TEST_DATA](const Block& block)
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
        "CREATE TABLE IF NOT EXISTS "
        "test_clickhouse_cpp.decimal (id UInt64, d1 Decimal(9, 4), d2 Decimal(18, 9), d3 Decimal(38, 19), "
        "                         d4 Decimal32(4), d5 Decimal64(9), d6 Decimal128(19)) "
        "ENGINE = Memory");

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

        client_->Insert("test_clickhouse_cpp.decimal", b);
    }

    client_->Select("SELECT id, d1, d2, d3, d4, d5, d6 FROM test_clickhouse_cpp.decimal ORDER BY id", [](const Block& b) {
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

// Test roundtrip of DateTime64 values
TEST_P(ClientCase, DateTime64) {
    const auto & server_info = client_->GetServerInfo();
    if (versionNumber(server_info) < versionNumber(20, 1)) {
        std::cout << "Test is skipped since server '" << server_info << "' does not support DateTime64" << std::endl;
        return;
    }

    Block block;
    client_->Execute("DROP TABLE IF EXISTS test_clickhouse_cpp.datetime64;");

    client_->Execute("CREATE TABLE IF NOT EXISTS "
            "test_clickhouse_cpp.datetime64 (dt DateTime64(6)) "
            "ENGINE = Memory");

    auto col_dt64 = std::make_shared<ColumnDateTime64>(6);
    block.AppendColumn("dt", col_dt64);

    // Empty INSERT and SELECT
    client_->Insert("test_clickhouse_cpp.datetime64", block);
    client_->Select("SELECT dt FROM test_clickhouse_cpp.datetime64",
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
    client_->Insert("test_clickhouse_cpp.datetime64", block);

    size_t total_rows = 0;
    client_->Select("SELECT dt FROM test_clickhouse_cpp.datetime64",
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

INSTANTIATE_TEST_CASE_P(
    Client, ClientCase,
    ::testing::Values(
        ClientOptions()
            .SetHost("localhost")
            .SetPingBeforeQuery(true),
        ClientOptions()
            .SetHost("localhost")
            .SetPingBeforeQuery(false)
            .SetCompressionMethod(CompressionMethod::LZ4)
    ));
