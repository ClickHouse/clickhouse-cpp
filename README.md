ClickHouse C++ client [![Linux](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/linux.yml/badge.svg)](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/linux.yml) [![macOS](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/macos.yml/badge.svg)](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/macos.yml)
=====

C++ client for [ClickHouse](https://clickhouse.com/).

## Supported data types

* Array(T)
* Date
* DateTime, DateTime64
* DateTime([timezone]), DateTime64(N, [timezone])
* Decimal32, Decimal64, Decimal128
* Enum8, Enum16
* FixedString(N)
* Float32, Float64
* IPv4, IPv6
* Nullable(T)
* String
* LowCardinality(String) or LowCardinality(FixedString(N))
* Tuple
* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* Int128
* UUID

## Building

```sh
$ mkdir build .
$ cd build
$ cmake .. [-DBUILD_TESTS=ON]
$ make
```

## Example

```cpp
#include <clickhouse/client.h>

using namespace clickhouse;

/// Initialize client connection.
Client client(ClientOptions().SetHost("localhost"));

/// Create a table.
client.Execute("CREATE TABLE IF NOT EXISTS test.numbers (id UInt64, name String) ENGINE = Memory");

/// Insert some values.
{
    Block block;

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    id->Append(7);

    auto name = std::make_shared<ColumnString>();
    name->Append("one");
    name->Append("seven");

    block.AppendColumn("id"  , id);
    block.AppendColumn("name", name);

    client.Insert("test.numbers", block);
}

/// Select values inserted in the previous step.
client.Select("SELECT id, name FROM test.numbers", [] (const Block& block)
    {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                      << block[1]->As<ColumnString>()->At(i) << "\n";
        }
    }
);

/// Delete table.
client.Execute("DROP TABLE test.numbers");
```
