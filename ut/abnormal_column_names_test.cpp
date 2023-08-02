
#include <clickhouse/columns/column.h>
#include <clickhouse/block.h>

#include "utils.h"

#include <gtest/gtest.h>

#include <unordered_set>
#include <iostream>

namespace {
using namespace clickhouse;

std::string getColumnNames(const Block& block) {
    std::string result;
    for (size_t i = 0; i < block.GetColumnCount(); ++i) {
        result += block.GetColumnName(i);
        if (i != block.GetColumnCount() - 1)
            result += ',';
    }

    return result;
}
}

struct AbnormalColumnNamesClientTestCase {
    ClientOptions client_options;
    std::vector<std::string> queries;
    std::vector<std::string> expected_names;
};

class AbnormalColumnNamesClientTest : public testing::TestWithParam<AbnormalColumnNamesClientTestCase> {
protected:
    void SetUp() override {
        client_ = std::make_unique<Client>(GetParam().client_options);
    }
    void TearDown() override {
        client_.reset();
    }

    std::unique_ptr<clickhouse::Client> client_;
};


TEST_P(AbnormalColumnNamesClientTest, Select) {
    const auto & queries = GetParam().queries;
    for (size_t i = 0; i < queries.size(); ++i) {

        const auto & query = queries.at(i);
        const auto & expected = GetParam().expected_names[i];

        client_->Select(query, [query, expected](const Block& block) {
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
                return;

            EXPECT_EQ(1UL, block.GetRowCount());

            EXPECT_EQ(expected, getColumnNames(block))
                << "For query: " << query;
        });
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
            {"select 123,231,113", "select 'ABC','AAA','BBB','CCC'"},
            {"123,231,113", "'ABC','AAA','BBB','CCC'"},
    }
));

