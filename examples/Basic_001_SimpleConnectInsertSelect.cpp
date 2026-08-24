#include <cstdlib>
#include <iostream>
#include <memory>
#include <clickhouse/client.h>

namespace ch = clickhouse;

int main()
{

    try {

        // Setup the client configuration options
        auto client_options = ch::ClientOptions{}
            .SetHost("you.clickhouse.host.com")    // Set your clickhouse host here
            .SetUser("the_user")                   // Set your username here
            .SetPassword("the#password1")          // Set your password here
            .SetSSLOptions({})                     // Remove this line for plain-text connection,
            .SetPort(9440);                        // and set port to 9000

        // Create the client instance with the options above
        auto client = ch::Client{client_options};


        // Create a test table
        client.Execute(R"(
            CREATE OR REPLACE TABLE greetings (
                id UInt64,
                message String,
                language String)
            ENGINE = MergeTree ORDER BY id)");

        // Create the columns to insert into the newly created table
        auto id = std::make_shared<clickhouse::ColumnUInt64>();
        auto message = std::make_shared<clickhouse::ColumnString>();
        auto language = std::make_shared<clickhouse::ColumnString>();

        // Add new the records to the column
        id->Append(1);
        message->Append("Hello, World!");
        language->Append("English");

        id->Append(2);
        message->Append("¡Hola, Mundo!");
        language->Append("Spanish");

        id->Append(3);
        message->Append("Hallo wereld!");
        language->Append("Dutch");

        // Form a block to send it with the insert statement
        ch::Block block{};
        block.AppendColumn("id", id);
        block.AppendColumn("message", message);
        block.AppendColumn("language", language);

        // Insert the data into the table using ClickHouse Native protocol
        client.Insert("greetings", block);


        client.BeginSelect("SELECT id, message, language FROM greetings");
        while (auto block = client.NextBlock()) {
            auto id = block->At(0)->AsStrict<ch::ColumnUInt64>();
            auto message = block->At(1)->AsStrict<ch::ColumnString>();
            auto language = block->At(2)->AsStrict<ch::ColumnString>();

            for (size_t i = 0; i < block->GetRowCount(); ++i) {
                std::cout << id->At(i) << "\t"
                          << message->At(i) << "\t"
                          << language->At(i) << "\n";
            }
        }

        return EXIT_SUCCESS;
    }
    catch (const std::exception & ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }
    catch (...) {
        std::cerr << "Unknown exception\n";
        return EXIT_FAILURE;
    }
}
