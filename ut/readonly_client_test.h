#pragma once

#include <clickhouse/client.h>

#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <vector>

class ReadonlyClientTest : public testing::TestWithParam<
        std::tuple<clickhouse::ClientOptions, std::vector<std::string> > /*queries*/> {
protected:
    void SetUp() override;
    void TearDown() override;

    std::unique_ptr<clickhouse::Client> client_;
};
