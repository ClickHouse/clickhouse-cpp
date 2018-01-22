#include <clickhouse/client.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

// Use value-parameterized tests to run same tests with different client
// options.
class ClientCase : public testing::TestWithParam<ClientOptions> {
protected:
    void SetUp() override {
        client_ = new Client(GetParam());
        client_->Execute("CREATE DATABASE IF NOT EXISTS test");
    }

    void TearDown() override {
        client_->Execute("DROP DATABASE test");
        delete client_;
    }

    Client* client_ = nullptr;
};

TEST_P(ClientCase, Array) {
    Block b;

    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.array (arr Array(UInt64)) "
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
        client_->Insert("test.array", b);
    }

    const uint64_t ARR_SIZE[] = { 1, 2, 3, 4 };
    const uint64_t VALUE[] = { 1, 3, 7, 9 };
    size_t row = 0;
    client_->Select("SELECT arr FROM test.array",
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
            "CREATE TABLE IF NOT EXISTS test.date (d DateTime) "
            "ENGINE = Memory");

    auto d = std::make_shared<ColumnDateTime>();
    auto const now = std::time(nullptr);
    d->Append(now);
    b.AppendColumn("d", d);
    client_->Insert("test.date", b);

    client_->Select("SELECT d FROM test.date", [&now](const Block& block)
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

TEST_P(ClientCase, Generic) {
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.client (id UInt64, name String) "
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

        client_->Insert("test.client", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, name FROM test.client", [TEST_DATA, &row](const Block& block)
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
            "CREATE TABLE IF NOT EXISTS test.nullable (id Nullable(UInt64), date Nullable(Date)) "
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

        client_->Insert("test.nullable", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, date FROM test.nullable",
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

TEST_P(ClientCase, Cancelable) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.cancel (x UInt64) "
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
        client_->Insert("test.cancel", b);
    }

    /// Send a query which is canceled after receiving the first blockr.
    int row_cnt = 0;
    EXPECT_NO_THROW(
        client_->SelectCancelable("SELECT * FROM test.cancel",
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
            "CREATE TABLE IF NOT EXISTS test.exceptions (id UInt64, name String) "
            "ENGINE = Memory");

    /// Expect failing on table creation.
    EXPECT_THROW(
        client_->Execute(
            "CREATE TABLE test.exceptions (id UInt64, name String) "
            "ENGINE = Memory"),
        ServerException);
}

TEST_P(ClientCase, Enum) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.enums (id UInt64, e Enum8('One' = 1, 'Two' = 2)) "
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

        client_->Insert("test.enums", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, e FROM test.enums", [&row, TEST_DATA](const Block& block)
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

