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

std::string getEnvOrDefault(const std::string& env, const std::string& default_val)
{
    const char* v = std::getenv(env.c_str());
    return v ? v : default_val;
}

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


inline void GenericExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_client (id UInt64, name String)");

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

        client.Insert("test_client", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM test_client", [](const Block& block)
        {
            PrintBlock(block);
        }
    );

    /// Delete table.
    client.Execute("DROP TEMPORARY TABLE test_client");
}


static void RunTests(Client& client) {
    GenericExample(client);
}

int main() {
    try {
        {
            Client client(ClientOptions()
                            .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
                            .SetPort( std::stoi(getEnvOrDefault("CLICKHOUSE_PORT",     "9000")))
                            .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
                            .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
                            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"))
                            .SetPingBeforeQuery(true));
            RunTests(client);
        }

    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
