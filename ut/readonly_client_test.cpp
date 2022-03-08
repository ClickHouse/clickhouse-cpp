#include "readonly_client_test.h"
#include "utils.h"

#include <clickhouse/columns/column.h>
#include <clickhouse/block.h>

#include <iostream>

namespace {
    using namespace clickhouse;
}

void ReadonlyClientTest::SetUp() {
    client_ = std::make_unique<Client>(std::get<0>(GetParam()));
}

void ReadonlyClientTest::TearDown() {
    client_.reset();
}

// Sometimes gtest fails to detect that this test is instantiated elsewhere, suppress the error explicitly.
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(ReadonlyClientTest);
TEST_P(ReadonlyClientTest, Select) {

    const auto & queries = std::get<1>(GetParam());
    for (const auto & query : queries) {
        client_->Select(query,
            [& query](const Block& block) {
                if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
                    return;
                std::cout << query << " => "
                          << "\n\trows: " << block.GetRowCount()
                          << ", columns: " << block.GetColumnCount()
                          << ", data:\n" << PrettyPrintBlock{block} << std::endl;
            }
        );
    }
}
