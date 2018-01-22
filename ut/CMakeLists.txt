ADD_EXECUTABLE (clickhouse-cpp-ut
    main.cpp

    columns_ut.cpp
    types_ut.cpp
    type_parser_ut.cpp
    client_ut.cpp
)

TARGET_LINK_LIBRARIES (clickhouse-cpp-ut
    clickhouse-cpp-lib
    gtest-lib
)
