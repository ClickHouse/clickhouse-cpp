#pragma once

#include <clickhouse/client.h>

#include <gtest/gtest.h>

#include <tuple>
#include <string>

// Just a wrapper to stand out from strings.
struct ExpectingException {
    std::string exception_message;
};

/// Verify that connection fails with some specific message.
class ConnectionFailedClientTest : public testing::TestWithParam<
        std::tuple<clickhouse::ClientOptions, ExpectingException>> {};
