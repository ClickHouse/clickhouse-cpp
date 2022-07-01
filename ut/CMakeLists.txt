SET ( clickhouse-cpp-ut-src
    main.cpp

    block_ut.cpp
    client_ut.cpp
    columns_ut.cpp
    column_array_ut.cpp
    itemview_ut.cpp
    socket_ut.cpp
    stream_ut.cpp
    type_parser_ut.cpp
    types_ut.cpp
    utils_ut.cpp

    performance_tests.cpp
    tcp_server.cpp
    readonly_client_test.cpp
    connection_failed_client_test.cpp
    array_of_low_cardinality_tests.cpp
    CreateColumnByType_ut.cpp
    Column_ut.cpp
    roundtrip_column.cpp

    utils.cpp
    value_generators.cpp
    low_cardinality_nullable_tests.cpp
)

IF (WITH_OPENSSL)
    LIST (APPEND clickhouse-cpp-ut-src ssl_ut.cpp)
ENDIF ()

ADD_EXECUTABLE (clickhouse-cpp-ut
    ${clickhouse-cpp-ut-src}
)

TARGET_LINK_LIBRARIES (clickhouse-cpp-ut
    clickhouse-cpp-lib-static
    gtest-lib
)
IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --rtlib=compiler-rt")
ENDIF ()
