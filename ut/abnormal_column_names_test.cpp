#include "abnormal_column_names_test.h"
#include "utils.h"

#include <clickhouse/columns/column.h>
#include <clickhouse/block.h>
#include <unordered_set>
#include <iostream>

namespace {
    using namespace clickhouse;
}

void AbnormalColumnNamesTest::SetUp() {
    client_ = std::make_unique<Client>(std::get<0>(GetParam()));
}

void AbnormalColumnNamesTest::TearDown() {
    client_.reset();
}

// Sometimes gtest fails to detect that this test is instantiated elsewhere, suppress the error explicitly.
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(AbnormalColumnNamesTest);
TEST_P(AbnormalColumnNamesTest, Select) {

    const auto & queries = std::get<1>(GetParam());
    for (const auto & query : queries) {
        std::unordered_set<std::string> names;
        size_t count = 0;
        client_->Select(query,
            [& query,& names, & count](const Block& block) {
                if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
                    return;

                std::cout << "query => " << query <<"\n" << PrettyPrintBlock{block}; 
                for (size_t i = 0; i < block.GetColumnCount(); ++i)
                {
                    count++;
                    names.insert(block.GetColumnName(i));
                }
            }
        );
        EXPECT_EQ(count, names.size());
        for(auto& name: names) {
            std::cout << name << ", count=" << count<< std::endl;
        }
    }
}
