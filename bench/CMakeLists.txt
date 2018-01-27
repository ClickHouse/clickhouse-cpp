ADD_EXECUTABLE (bench
    bench.cpp
)

TARGET_LINK_LIBRARIES (bench
    clickhouse-cpp-lib
    benchmark
)
