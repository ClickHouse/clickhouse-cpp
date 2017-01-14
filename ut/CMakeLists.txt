ADD_EXECUTABLE (clickhouse-cpp-ut
    main.cpp

    type_parser_ut.cpp
)

TARGET_LINK_LIBRARIES (clickhouse-cpp-ut
    clickhouse-cpp-lib
    gtest-lib
)
