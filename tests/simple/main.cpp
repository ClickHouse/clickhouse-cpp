#include <clickhouse/client.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/types/type_parser.h>

#include <stdexcept>
#include <iostream>
#include <cmath>

#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif

using namespace clickhouse;
using namespace std;

inline void PrintBlock(const Block& block) {
    for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        std::cout << bi.Name() << " ";
    }
    std::cout << std::endl;

    for (size_t i = 0; i < block.GetRowCount(); ++i) {
        std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
                  << (*block[1]->As<ColumnString>())[i] << "\n";
    }
}

inline void ArrayExample(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.array (arr Array(UInt64)) ENGINE = Memory");

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
    client.Insert("test.array", b);

    client.Select("SELECT arr FROM test.array", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnArray>()->GetAsColumn(c);
                for (size_t i = 0; i < col->Size(); ++i) {
                    std::cerr << (int)(*col->As<ColumnUInt64>())[i] << " ";
                }
                std::cerr << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.array");
}

inline void MultiArrayExample(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.multiarray (arr Array(Array(UInt64))) ENGINE = Memory");

    auto arr = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(17);
    arr->AppendAsColumn(id);

    auto a2 = std::make_shared<ColumnArray>(std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>()));
    a2->AppendAsColumn(arr);
    b.AppendColumn("arr", a2);
    client.Insert("test.multiarray", b);

    client.Select("SELECT arr FROM test.multiarray", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnArray>()->GetAsColumn(c);
                cout << "[";
                for (size_t i = 0; i < col->Size(); ++i) {
                    auto col2 = col->As<ColumnArray>()->GetAsColumn(i);
                    for (size_t j = 0; j < col2->Size(); ++j) {
                        cout << (int)(*col2->As<ColumnUInt64>())[j];
                        if (j + 1 != col2->Size()) {
                            cout << " ";
                        }
                    }
                }
                std::cout << "]" << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.multiarray");
}

inline void DateExample(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.date (d DateTime) ENGINE = Memory");

    auto d = std::make_shared<ColumnDateTime>();
    d->Append(std::time(nullptr));
    b.AppendColumn("d", d);
    client.Insert("test.date", b);

    client.Select("SELECT d FROM test.date", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnDateTime>();
                std::time_t t = col->At(c);
                std::cerr << std::asctime(std::localtime(&t)) << " " << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.date");
}

inline void DateTime64Example(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.datetime64 (dt64 DateTime64(6)) ENGINE = Memory");

    size_t precision = 6ul;
    auto d = std::make_shared<ColumnDateTime64>(precision);
    assert(d->GetPrecision() == precision);
    Int64 precision_multiplier = std::pow(10ull, precision);
    Int64 datetime = Int64(std::time(nullptr)) * precision_multiplier;

    d->Append(datetime);
    b.AppendColumn("dt64", d);
    client.Insert("test.date", b);

    client.Select("SELECT d FROM test.datetime64", [precision_multiplier](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnDateTime64>();
                const time_t t = static_cast<time_t>(col->At(c) / precision_multiplier);
                std::cerr << std::asctime(std::localtime(&t)) << " " << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.datetime64");
}

inline void DecimalExample(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.decimal (d Decimal64(4)) ENGINE = Memory");

    auto d = std::make_shared<ColumnDecimal>(18, 4);
    d->Append(21111);
    b.AppendColumn("d", d);
    client.Insert("test.decimal", b);

    client.Select("SELECT d FROM test.decimal", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnDecimal>();
                cout << (int)col->At(c) << endl;
            }
        }
    );

    client.Select("SELECT toDecimal32(2, 4) AS x", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnDecimal>();
                cout << (int)col->At(c) << endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.decimal");
}

inline void GenericExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.client (id UInt64, name String) ENGINE = Memory");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(7);

        auto name = std::make_shared<ColumnString>();
        name->Append("one");
        name->Append("seven");

        block.AppendColumn("id"  , id);
        block.AppendColumn("name", name);

        client.Insert("test.client", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM test.client", [](const Block& block)
        {
            PrintBlock(block);
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.client");
}

inline void NullableExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.client (id Nullable(UInt64), date Nullable(Date)) ENGINE = Memory");

    /// Insert some values.
    {
        Block block;

        {
            auto id = std::make_shared<ColumnUInt64>();
            id->Append(1);
            id->Append(2);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(0);

            block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
        }

        {
            auto date = std::make_shared<ColumnDate>();
            date->Append(std::time(nullptr));
            date->Append(std::time(nullptr));

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("date", std::make_shared<ColumnNullable>(date, nulls));
        }

        client.Insert("test.client", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, date FROM test.client", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col_id   = block[0]->As<ColumnNullable>();
                auto col_date = block[1]->As<ColumnNullable>();

                if (col_id->IsNull(c)) {
                    std::cerr << "\\N ";
                } else {
                    std::cerr << col_id->Nested()->As<ColumnUInt64>()->At(c)
                              << " ";
                }

                if (col_date->IsNull(c)) {
                    std::cerr << "\\N\n";
                } else {
                    std::time_t t = col_date->Nested()->As<ColumnDate>()->At(c);
                    std::cerr << std::asctime(std::localtime(&t))
                              << "\n";
                }
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.client");
}

inline void NumbersExample(Client& client) {
    size_t num = 0;

    client.Select("SELECT number, number FROM system.numbers LIMIT 100000", [&num](const Block& block)
        {
            if (Block::Iterator(block).IsValid()) {
                auto col = block[0]->As<ColumnUInt64>();

                for (size_t i = 0; i < col->Size(); ++i) {
                    if (col->At(i) < num) {
                        throw std::runtime_error("invalid sequence of numbers");
                    }

                    num = col->At(i);
                }
            }
        }
    );
}

inline void CancelableExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.client (x UInt64) ENGINE = Memory");

    /// Insert a few blocks.
    for (unsigned j = 0; j < 10; j++) {
        Block b;

        auto x = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < 1000; i++) {
            x->Append(i);
        }

        b.AppendColumn("x", x);
        client.Insert("test.client", b);
    }

    /// Send a query which is canceled after receiving the first block (note:
    /// due to the low number of rows in this test, this will not actually have
    /// any effect, it just tests for errors)
    client.SelectCancelable("SELECT * FROM test.client", [](const Block&)
        {
            return false;
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.client");
}

inline void ExecptionExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.exceptions (id UInt64, name String) ENGINE = Memory");
    /// Expect failing on table creation.
    try {
        client.Execute("CREATE TABLE test.exceptions (id UInt64, name String) ENGINE = Memory");
    } catch (const ServerException& e) {
        if (e.GetCode() == ErrorCodes::TABLE_ALREADY_EXISTS) {
            // OK
        } else {
            throw;
        }
    }

    /// Delete table.
    client.Execute("DROP TABLE test.exceptions");
}

inline void EnumExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.enums (id UInt64, e Enum8('One' = 1, 'Two' = 2)) ENGINE = Memory");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);

        auto e = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"One", 1}, {"Two", 2}}));
        e->Append(1);
        e->Append("Two");

        block.AppendColumn("id", id);
        block.AppendColumn("e", e);

        client.Insert("test.enums", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, e FROM test.enums", [](const Block& block)
        {
            for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
                std::cout << bi.Name() << " ";
            }
            std::cout << std::endl;

            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
                          << (*block[1]->As<ColumnEnum8>()).NameAt(i) << "\n";
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.enums");
}

inline void SelectNull(Client& client) {
    client.Select("SELECT NULL", []([[maybe_unused]] const Block& block)
        {
            assert(block.GetRowCount() < 2);
            (void)(block);
        }
    );
}

inline void ShowTables(Client& client) {
    /// Select values inserted in the previous step.
    client.Select("SHOW TABLES", [](const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnString>())[i] << "\n";
            }
        }
    );
}

inline void IPExample(Client &client) {
    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS test.ips (id UInt64, v4 IPv4, v6 IPv6) ENGINE = Memory");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);
        id->Append(3);

        auto v4 = std::make_shared<ColumnIPv4>();
        v4->Append("127.0.0.1");
        v4->Append(3585395774);
        v4->Append(0);

        auto v6 = std::make_shared<ColumnIPv6>();
        v6->Append("::1");
        v6->Append("aa::ff");
        v6->Append("fe80::86ba:ef31:f2d8:7e8b");

        block.AppendColumn("id", id);
        block.AppendColumn("v4", v4);
        block.AppendColumn("v6", v6);

        client.Insert("test.ips", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, v4, v6 FROM test.ips", [](const Block& block)
        {
            for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
                std::cout << bi.Name() << " ";
            }
            std::cout << std::endl;

            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
                          << (*block[1]->As<ColumnIPv4>()).AsString(i) << " (" << (*block[1]->As<ColumnIPv4>())[i].s_addr << ") "
                          << (*block[2]->As<ColumnIPv6>()).AsString(i) << "\n";
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.ips");
}

static void RunTests(Client& client) {
    ArrayExample(client);
    CancelableExample(client);
    DateExample(client);
    DateTime64Example(client);
    DecimalExample(client);
    EnumExample(client);
    ExecptionExample(client);
    GenericExample(client);
    IPExample(client);
    MultiArrayExample(client);
    NullableExample(client);
    NumbersExample(client);
    SelectNull(client);
    ShowTables(client);
}

int main() {
    try {
        {
            Client client(ClientOptions()
                            .SetHost("localhost")
                            .SetPingBeforeQuery(true));
            RunTests(client);
        }

        {
            Client client(ClientOptions()
                            .SetHost("localhost")
                            .SetPingBeforeQuery(true)
                            .SetCompressionMethod(CompressionMethod::LZ4));
            RunTests(client);
        }
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
