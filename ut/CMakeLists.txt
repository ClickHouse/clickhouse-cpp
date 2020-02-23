ADD_EXECUTABLE (clickhouse-cpp-ut
    main.cpp

    client_ut.cpp
    columns_ut.cpp
    socket_ut.cpp
    stream_ut.cpp
    tcp_server.cpp
    type_parser_ut.cpp
    types_ut.cpp
)

TARGET_LINK_LIBRARIES (clickhouse-cpp-ut
    clickhouse-cpp-lib
    gtest-lib
)
