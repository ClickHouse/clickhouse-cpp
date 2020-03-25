ADD_EXECUTABLE (clickhouse-cpp-ut
    main.cpp

    client_ut.cpp
    columns_ut.cpp
    socket_ut.cpp
    stream_ut.cpp
    type_parser_ut.cpp
    types_ut.cpp

    performance_tests.cpp
    tcp_server.cpp
    utils.cpp
)

TARGET_LINK_LIBRARIES (clickhouse-cpp-ut
    clickhouse-cpp-lib
    gtest-lib
)
