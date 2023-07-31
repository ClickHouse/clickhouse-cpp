#include "abnormal_column_names_test.h"
#include "utils.h"

#include <clickhouse/columns/column.h>
#include <clickhouse/block.h>
#include <unordered_set>
#include <iostream>

namespace {
    using namespace clickhouse;
}

void AbnormalColumnNamesClientTest::SetUp() {
    client_ = std::make_unique<Client>(std::get<0>(GetParam()));
}

void AbnormalColumnNamesClientTest::TearDown() {
    client_.reset();
}

// Sometimes gtest fails to detect that this test is instantiated elsewhere, suppress the error explicitly.
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(AbnormalColumnNamesClientTest);
TEST_P(AbnormalColumnNamesClientTest, Select) {
    // TODO(vnemkov): move expected results into the test parameters, also get rid of PrettyPrintBlock
    static const std::vector<std::string> expect_results {
        "+-------+-------+-------+\n"\
        "|   123 |   231 |   113 |\n"\
        "+-------+-------+-------+\n"\
        "| UInt8 | UInt8 | UInt8 |\n"\
        "+-------+-------+-------+\n"\
        "|   123 |   231 |   113 |\n"\
        "+-------+-------+-------+\n",
        "+--------+--------+--------+--------+\n"\
        "|  'ABC' |  'AAA' |  'BBB' |  'CCC' |\n"\
        "+--------+--------+--------+--------+\n"\
        "| String | String | String | String |\n"\
        "+--------+--------+--------+--------+\n"\
        "|    ABC |    AAA |    BBB |    CCC |\n"\
        "+--------+--------+--------+--------+\n"
    };
    const auto & queries = std::get<1>(GetParam());
    for (size_t i = 0; i < queries.size(); ++i) {
        const auto & query = queries.at(i);
        client_->Select(query,
            [& queries, i](const Block& block) {
                if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
                    return;
                EXPECT_EQ(1UL, block.GetRowCount());
                EXPECT_EQ(i == 0 ? 3UL: 4UL, block.GetColumnCount());

                std::stringstream sstr;
                sstr << PrettyPrintBlock{block}; 
                auto result = sstr.str(); 
                std::cout << "query => " << queries.at(i) <<"\n" << PrettyPrintBlock{block}; 
                ASSERT_EQ(expect_results.at(i), result);
            }
        );
    }
}


INSTANTIATE_TEST_SUITE_P(ClientColumnNames, AbnormalColumnNamesClientTest,
    ::testing::Values(AbnormalColumnNamesClientTest::ParamType{
        ClientOptions()
            .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
            .SetPort(           getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
            .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
            .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None),
            {"select 123,231,113", "select 'ABC','AAA','BBB','CCC'"}
    }
));

