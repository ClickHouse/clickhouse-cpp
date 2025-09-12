ClickHouse C++ client [![Linux](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/linux.yml/badge.svg)](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/linux.yml) [![macOS](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/macos.yml/badge.svg)](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/macos.yml) [![Windows MSVC](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_msvc.yml/badge.svg)](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_msvc.yml) [![Windows mingw](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_mingw.yml/badge.svg)](https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_mingw.yml)
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
* UInt128, Int128
* UUID
* Map
* Point, Ring, Polygon, MultiPolygon

## Dependencies
In the most basic case one needs only:
- a C++-17-complaint compiler,
- `cmake` (3.12 or newer), and
- `ninja`

Optional dependencies:
- openssl
- liblz4
- libabsl
- libzstd

## Building

```sh
$ mkdir build .
$ cd build
$ cmake .. [-DBUILD_TESTS=ON]
$ make
```

Please refer to the workflows for the reference on dependencies/build options
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/linux.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/windows_msvc.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/windows_mingw.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/macos.yml


## Example application build with clickhouse-cpp

There are various ways to integrate clickhouse-cpp with the build system of an application. Below example uses the simple approach based on
submodules presented in https://www.youtube.com/watch?v=ED-WUk440qc .

- `mkdir clickhouse-app && cd clickhouse-app && git init`
- `git submodule add https://github.com/ClickHouse/clickhouse-cpp.git contribs/clickhouse-cpp`
- `touch app.cpp`, then copy the following C++ code into that file

```cpp
#include <iostream>
#include <clickhouse/client.h>

using namespace clickhouse;

int main()
{
    /// Initialize client connection.
    Client client(ClientOptions().SetHost("localhost"));

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS default.numbers (id UInt64, name String) ENGINE = Memory");

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

        client.Insert("default.numbers", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM default.numbers", [] (const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                          << block[1]->As<ColumnString>()->At(i) << "\n";
            }
        }
    );

    /// Select values inserted in the previous step using external data feature
    /// See https://clickhouse.com/docs/engines/table-engines/special/external-data
    {
        Block block1, block2;
        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        block1.AppendColumn("id"  , id);

        auto name = std::make_shared<ColumnString>();
        name->Append("seven");
        block2.AppendColumn("name", name);

        const std::string _1 = "_1";
        const std::string _2 = "_2";

        const ExternalTables external = {{_1, block1}, {_2, block2}};
        client.SelectWithExternalData("SELECT id, name FROM default.numbers where id in (_1) or name in (_2)",
                                      external, [] (const Block& block)
            {
                for (size_t i = 0; i < block.GetRowCount(); ++i) {
                    std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                              << block[1]->As<ColumnString>()->At(i) << "\n";
                }
            }
        );
    }

    /// Delete table.
    client.Execute("DROP TABLE default.numbers");

    return 0;
}
```

- `touch CMakeLists.txt`, then copy the following CMake code into that file

```cmake
cmake_minimum_required(VERSION 3.12)
project(application-example)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(contribs/clickhouse-cpp)

add_executable(${PROJECT_NAME} "app.cpp")

target_include_directories(${PROJECT_NAME} PRIVATE contribs/clickhouse-cpp/ contribs/clickhouse-cpp/contrib/absl)

target_link_libraries(${PROJECT_NAME} PRIVATE clickhouse-cpp-lib)
```

- run `rm -rf build && cmake -B build -S . && cmake --build build -j32` to remove remainders of the previous builds, run CMake and build the
  application. The generated binary is located in location `build/application-example`.

## Thread-safety
⚠ Please note that `Client` instance is NOT thread-safe. I.e. you must create a separate `Client` for each thread or utilize some synchronization techniques. ⚠

## Retries
If you wish to implement some retry logic atop of `clickhouse::Client` there are few simple rules to make you life easier:
- If previous attempt threw an exception, then make sure to call `clickhouse::Client::ResetConnection()` before the next try.
- For `clickhouse::Client::Insert()` you can reuse a block from previous try, no need to rebuild it from scratch.

See https://github.com/ClickHouse/clickhouse-cpp/issues/184 for details.

## Asynchronous inserts
See https://clickhouse.com/docs/en/cloud/bestpractices/asynchronous-inserts for details.

⚠ The asynchronous setting is different according to the clickhouse-server version. The under example with clickhouse-server version 24.8.4.13. ⚠

> Our strong recommendation is to use async_insert=1,wait_for_async_insert=1 if using asynchronous inserts. Using wait_for_async_insert=0 is very risky because your INSERT client may not be aware if there are errors, and also can cause potential overload if your client continues to write quickly in a situation where the ClickHouse server needs to slow down the writes and create some backpressure in order to ensure reliability of the service.

- Only use the SDK, do not need to change the clickhouse-server config. Asynchronous inserts only work if the data is sent as SQL text format. Here is the example.
```cpp
// You can specify the asynchronous insert settings by using the SETTINGS clause of insert queries
clickhouse::Query query("INSERT INTO default.test SETTINGS async_insert=1,wait_for_async_insert=1,async_insert_busy_timeout_ms=5000,async_insert_use_adaptive_busy_timeout=0,async_insert_max_data_size=104857600 VALUES(10,10)");
client.Execute(query);

// Or by SetSetting
clickhouse::Query query("INSERT INTO default.test VALUES(10,10)");
query.SetSetting("async_insert", clickhouse::QuerySettingsField{ "1", 1 });
query.SetSetting("wait_for_async_insert", clickhouse::QuerySettingsField{ "1", 1 }); // strong recommendation
query.SetSetting("async_insert_busy_timeout_ms", clickhouse::QuerySettingsField{ "5000", 1 });
query.SetSetting("async_insert_max_data_size", clickhouse::QuerySettingsField{ "104857600", 1 });
query.SetSetting("async_insert_use_adaptive_busy_timeout", clickhouse::QuerySettingsField{ "0", 1 });
client.Execute(query);

// Not available case. The Insert interface actually use the native data format
clickhouse::Block block;
client.Insert("default.test", block);
```
- Change the clickhouse-server users.xml, enable asynchronous inserts (available for the native data format). Here is the example.
```xml
<profiles>
        <!-- Default settings. -->
        <default>
            <async_insert>1</async_insert>
            <wait_for_async_insert>1</wait_for_async_insert>
            <async_insert_use_adaptive_busy_timeout>0</async_insert_use_adaptive_busy_timeout>
            <async_insert_busy_timeout_ms>5000</async_insert_busy_timeout_ms>
            <async_insert_max_data_size>104857600</async_insert_max_data_size>
        </default>

        <!-- Profile that allows only read queries. -->
        <readonly>
            <readonly>1</readonly>
        </readonly>
    </profiles>
```
- Enabling asynchronous inserts at the user level. Ensure your login account has the privileges about ALTER USER. Then you can use insert_account for asynchronous inserts.
```sql
ALTER USER insert_account SETTINGS async_insert=1,wait_for_async_insert=1,async_insert_use_adaptive_busy_timeout=0,async_insert_busy_timeout_ms=5000,async_insert_max_data_size=104857600
```


