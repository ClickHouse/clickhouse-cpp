#pragma once

#include <clickhouse/columns/column.h>

namespace clickhouse {
    class Client;
}

clickhouse::ColumnRef RoundtripColumnValues(clickhouse::Client& client, clickhouse::ColumnRef expected);
