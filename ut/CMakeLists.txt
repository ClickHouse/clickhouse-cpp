SET ( clickhouse-cpp-ut-src
    main.cpp

    client_ut.cpp
    columns_ut.cpp
    itemview_ut.cpp
    socket_ut.cpp
    stream_ut.cpp
    type_parser_ut.cpp
    types_ut.cpp

    performance_tests.cpp
    tcp_server.cpp
    utils.cpp
    readonly_client_ut.cpp
    connection_failed_client_ut.cpp
)

IF (WITH_OPENSSL)
    LIST (APPEND clickhouse-cpp-ut-src ssl_ut.cpp)
ENDIF ()

ADD_EXECUTABLE (clickhouse-cpp-ut
    ${clickhouse-cpp-ut-src}
)

TARGET_LINK_LIBRARIES (clickhouse-cpp-ut
    clickhouse-cpp-lib
    gtest-lib
)
