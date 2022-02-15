#include "connection_failed_client_test.h"
#include "utils.h"

#include <clickhouse/columns/column.h>
#include <clickhouse/block.h>

#include <memory>
#include <iostream>

namespace {
    using namespace clickhouse;
}

TEST_P(ConnectionFailedClientTest, ValidateConnectionError) {

    const auto & client_options = std::get<0>(GetParam());
    const auto & ee = std::get<1>(GetParam());

    std::unique_ptr<Client> client;
    try {
        client = std::make_unique<Client>(client_options);
        ASSERT_EQ(nullptr, client.get()) << "Connectiong established but it should have failed";
    } catch (const std::exception & e) {
        const auto message = std::string_view(e.what());
        ASSERT_TRUE(message.find(ee.exception_message) != std::string_view::npos)
                << "Expected exception message: " << ee.exception_message << "\n"
                << "Actual exception message  : " << e.what() << std::endl;
    }
}
