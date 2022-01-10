#pragma once

#include <clickhouse/client.h>

#include <gtest/gtest.h>

#include <tuple>
#include <string>

/// Verify that connection fails with some specific message.
class ConnectionFailedClientTest : public testing::TestWithParam<
        std::tuple<clickhouse::ClientOptions, std::string /*error message*/>> {};
