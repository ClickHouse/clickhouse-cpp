<h1 align="center">
<img src=".static/clickhouse-logo.svg" width="50px" align="center" style="margin-bottom: 10px;">
ClickHouse C++ Client
</h1>

<p align="center">
C++17 client library for <a href="https://clickhouse.com/">ClickHouse</a> using the native ClickHouse protocol.
</p>

`clickhouse-cpp` provides a small, direct API for connecting to ClickHouse, executing SQL queries,
inserting and selecting columnar `Block` data, and working with ClickHouse data types from C++. It
builds on Linux, macOS, and Windows with CMake or Bazel. It supports TLS and compression.

<br/>

<p align="center">
<a href="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/linux.yml"><img src="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/linux.yml/badge.svg" alt="Linux"></a>
<a href="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/macos.yml"><img src="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/macos.yml/badge.svg" alt="macOS"></a>
<a href="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_msvc.yml"><img src="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_msvc.yml/badge.svg" alt="Windows MSVC"></a>
<a href="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_mingw.yml"><img src="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/windows_mingw.yml/badge.svg" alt="Windows mingw"></a>
<a href="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/bazel.yml"><img src="https://github.com/ClickHouse/clickhouse-cpp/actions/workflows/bazel.yml/badge.svg" alt="Windows mingw"></a>
</p>


## Building

Here is an example with recommended settings;

```sh
$ mkdir build .
$ cd build
$ cmake .. -DCH_USE_ABSEIL_FOR_BIGNUM=NO -DCH_MAP_BOOL_TO_UINT8=NO
$ make
```

The command above disables two legacy CMake defaults, `CH_USE_ABSEIL_FOR_BIGNUM`  and
`CH_MAP_BOOL_TO_UINT8`. New projects should set both options to `OFF`. Existing projects can keep
the defaults temporarily, but should migrate to this configuration as this behavior will be removed
in the future versions of the library.

Please refer to the workflows for the reference on dependencies/build options
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/linux.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/windows_msvc.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/windows_mingw.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/macos.yml
- https://github.com/ClickHouse/clickhouse-cpp/blob/master/.github/workflows/bazel.yml

## Backwards compatibility

We aim to keep the public API compatible across releases within the same major version, so regular
source-level upgrades should not require application code changes unless explicitly documented.

ABI compatibility is not guaranteed yet, because the library is still actively evolving. If you
upgrade clickhouse-cpp, rebuild your application against the new library version, including for
minor version updates.

## Including clickhouse-cpp in your project

Here a simple example (`app.cpp`) of an application using `clickhouse-cpp`

```cpp
#include <clickhouse/client.h>
#include <iostream>

namespace ch = clickhouse;

int main()
{
    ch::Client client{ch::ClientOptions{}.SetHost("localhost")};

    client.BeginSelect("SELECT 'Hello from ClickHouse :)");
    
    while (auto block = client.NextBlock()) {
        auto col_msg = block->At(0)->AsStrict<ch::ColumnString>();
        
        for (size_t i = 0; i < block->GetRowCount(); ++i) {
            std::cout << col_msg->At(i) << "\n";
        }
    }
}
```

You can include `clickhouse-cpp` into your project by using one of the following methods.

### CMake with a git submodule

Add clickhouse-cpp as a submodule, for example under `contrib/clickhouse-cpp`:

```sh
git submodule add https://github.com/ClickHouse/clickhouse-cpp.git contrib/clickhouse-cpp
```

Then include it from your `CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.13)
project(application-example LANGUAGES CXX)

set(CH_USE_ABSEIL_FOR_BIGNUM OFF)
set(CH_MAP_BOOL_TO_UINT8 OFF)

add_subdirectory(contrib/clickhouse-cpp)

add_executable(application-example app.cpp)
target_link_libraries(application-example PRIVATE clickhouse-cpp-lib)
```

### CMake with FetchContent

You can also let CMake download clickhouse-cpp during configuration:

```cmake
cmake_minimum_required(VERSION 3.14)
project(application-example LANGUAGES CXX)

include(FetchContent)

set(CH_USE_ABSEIL_FOR_BIGNUM OFF)
set(CH_MAP_BOOL_TO_UINT8 OFF)

FetchContent_Declare(
    clickhouse_cpp
    GIT_REPOSITORY https://github.com/ClickHouse/clickhouse-cpp.git
    GIT_TAG v2.6.2
)
FetchContent_MakeAvailable(clickhouse_cpp)

add_executable(application-example app.cpp)
target_link_libraries(application-example PRIVATE clickhouse-cpp-lib)
```

### Bazel

Bazel support is experimental and uses bzlmod. Add clickhouse-cpp to your `MODULE.bazel`:

```bzl
bazel_dep(name = "clickhouse-cpp", version = "2.6.2")
```

Then depend on the library from your `BUILD.bazel`:

```bzl
cc_binary(
    name = "application-example",
    srcs = ["app.cpp"],
    deps = ["@clickhouse-cpp//:clickhouse"],
)
```

By default, the Bazel build uses BoringSSL for TLS because it builds reliably across platforms on
BCR and matches what many Bazel workspaces already link against. Use `--@clickhouse-cpp//:tls=openssl`
to build with OpenSSL, or `--@clickhouse-cpp//:tls=no` to build without TLS support.

Please keep in mind that Bazel support is currently experimental and is still being refined.
This means things might change as we improve and update the setup.

Most importantly, the project includes settings that were added for compatibility with older
versions of the library and its API, and to preserve old behavior. These settings will not be part
of the Bazel configuration. This means that even when only the minor version changes your build
still might break.

## Supported data types

* Array(T)
* Bool
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
* JSON

Important notes:
- With CMake Bool type is mapped to `clickhouse::ColumnUInt8` by default for backwards compatibility. Use
`-DCH_MAP_BOOL_TO_UINT8=OFF` to map Bool to `clickhouse::ColumnBool`.
- JSON requires requires setting `output_format_native_write_json_as_string=1 enabled for the query.

## Batch Insertion

In addition to the `Insert` method, which inserts all the data in a block in a
single call, you can use the `BeginInsert` / `SendInsertBlock` / `EndInsert`
pattern to insert batches of data. This can be useful for managing larger data
sets without inflating memory with the entire set.

To use it pass `BeginInsert` an `INSERT` statement ending in `VALUES` but with
no actual values. Use the resulting `Block` to append batches of data, sending
each to the sever with `SendInsertBlock`. Finally, call `EndInsert` (or let the
client go out of scope) to signal the server that insertion is complete.
Example:

```cpp
// Start the insertion.
auto block = client->BeginInsert("INSERT INTO foo (id, name) VALUES");

// Grab the columns from the block.
auto col1 = block[0]->As<ColumnUInt64>();
auto col2 = block[1]->As<ColumnString>();

// Add a couple of records to the block.
col1.Append(1);
col1.Append(2);
col2.Append("holden");
col2.Append("naomi");

// Send those records.
block.RefreshRowCount();
client->SendInsertBlock(block);
block.Clear();

// Add another record.
col1.Append(3);
col2.Append("amos");

// Send it and finish.
block.RefreshRowCount();
client->EndInsert();
```

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
