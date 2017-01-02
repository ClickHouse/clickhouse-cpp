ADD_LIBRARY (clickhouse-cpp-lib STATIC
    client.cpp
)

SET_TARGET_PROPERTIES(clickhouse-cpp-lib PROPERTIES LINKER_LANGUAGE CXX)