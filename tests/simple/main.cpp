#include <clickhouse/client.h>
#include <clickhouse/types/type_parser.h>

#include <iostream>

#if defined(_MSC_VER)
#	pragma warning(disable : 4996)
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
                std::time_t t = col->As<ColumnDateTime>()->At(c);
                std::cerr << std::asctime(std::localtime(&t)) << " " << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE test.date");
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

int main() {
    Client client(ClientOptions().SetHost("localhost"));

    try {
        ArrayExample(client);
        DateExample(client);
        GenericExample(client);
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
