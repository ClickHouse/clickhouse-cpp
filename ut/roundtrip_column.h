#pragma once

#include <clickhouse/columns/column.h>
#include <memory>

namespace clickhouse {
    class Client;
}

clickhouse::ColumnRef RoundtripColumnValues(clickhouse::Client& client, clickhouse::ColumnRef expected);

template <typename T>
auto RoundtripColumnValuesTyped(clickhouse::Client& client, std::shared_ptr<T> expected_col)
{
    return RoundtripColumnValues(client, expected_col)->template As<T>();
}
