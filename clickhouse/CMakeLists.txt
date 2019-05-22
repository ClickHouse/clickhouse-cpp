SET ( clickhouse-cpp-lib-src
    base/coded.cpp
    base/compressed.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp

    columns/array.cpp
    columns/date.cpp
    columns/enum.cpp
    columns/factory.cpp
    columns/nullable.cpp
    columns/numeric.cpp
    columns/string.cpp
    columns/tuple.cpp
    columns/uuid.cpp

    types/type_parser.cpp
    types/types.cpp

    block.cpp
    client.cpp
    query.cpp
)

ADD_LIBRARY (clickhouse-cpp-lib SHARED ${clickhouse-cpp-lib-src})

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)

TARGET_LINK_LIBRARIES (clickhouse-cpp-lib
    cityhash-lib
    lz4-lib
)

ADD_LIBRARY (clickhouse-cpp-lib-static STATIC ${clickhouse-cpp-lib-src})
TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static
    cityhash-lib
    lz4-lib
)