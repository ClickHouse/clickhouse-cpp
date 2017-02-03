ADD_LIBRARY (clickhouse-cpp-lib STATIC
    base/coded.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp

    block.cpp
    client.cpp
    columns.cpp
    query.cpp
    type_parser.cpp
)

TARGET_INCLUDE_DIRECTORIES(clickhouse-cpp-lib
    PRIVATE .
)

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)
