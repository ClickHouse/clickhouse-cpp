ADD_LIBRARY (clickhouse-cpp-lib
    base/coded.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp

    columns/array.cpp
    columns/date.cpp
    columns/factory.cpp
    columns/nullable.cpp
    columns/numeric.cpp
    columns/string.cpp
    columns/tuple.cpp

    types/type_parser.cpp
    types/types.cpp

    block.cpp
    client.cpp
    query.cpp
)

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)
