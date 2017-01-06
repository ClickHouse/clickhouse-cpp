ADD_LIBRARY (clickhouse-cpp-lib STATIC
    base/platform.cpp
    io/coded_input.cpp
    io/input.cpp
    io/output.cpp
    net/socket.cpp
    client.cpp
)

TARGET_INCLUDE_DIRECTORIES(clickhouse-cpp-lib
	PRIVATE .
)

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)
