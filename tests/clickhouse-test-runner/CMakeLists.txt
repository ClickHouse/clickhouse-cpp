# clickhouse-test-runner: a clickhouse-client imitation that runs the upstream
# ClickHouse SQL tests (tests/queries/0_stateless) through clickhouse-cpp. See
# README.md. The root CMakeLists.txt adds this directory only with
# -DBUILD_CLICKHOUSE_TEST_RUNNER=ON, so the normal build does not change.

IF (CH_MAP_BOOL_TO_UINT8)
    MESSAGE (FATAL_ERROR
        "BUILD_CLICKHOUSE_TEST_RUNNER needs CH_MAP_BOOL_TO_UINT8=OFF (for example -DCH_USE_3X_API=ON): "
        "the server writes Bool values as true and false, and a ColumnUInt8 cannot tell Bool from UInt8.")
ENDIF ()

ADD_LIBRARY (clickhouse-test-runner-lib STATIC
    args.cpp
    error_codes.cpp
    format.cpp
    hint.cpp
    log.cpp
    render.cpp
    runner.cpp
    shim.cpp
    split.cpp
    sql_text.cpp
)
TARGET_INCLUDE_DIRECTORIES (clickhouse-test-runner-lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
TARGET_LINK_LIBRARIES (clickhouse-test-runner-lib PUBLIC clickhouse-cpp-lib)

# tests/clickhouse-test runs `clickhouse` and `clickhouse-client`; the run
# script links both names to this program.
ADD_EXECUTABLE (clickhouse-test-runner main.cpp)
TARGET_LINK_LIBRARIES (clickhouse-test-runner clickhouse-test-runner-lib)
SET_TARGET_PROPERTIES (clickhouse-test-runner PROPERTIES OUTPUT_NAME clickhouse)

# The unit tests of the shim, and the live tests (CLICKHOUSE_TEST_RUNNER_LIVE=1)
# that compare its output with the server's TabSeparated output. They are a
# separate program, not part of clickhouse-cpp-ut.
IF (NOT TARGET gtest-lib)
    INCLUDE_DIRECTORIES (${PROJECT_SOURCE_DIR}/contrib/gtest/include ${PROJECT_SOURCE_DIR}/contrib/gtest)
    ADD_SUBDIRECTORY (${PROJECT_SOURCE_DIR}/contrib/gtest ${CMAKE_CURRENT_BINARY_DIR}/gtest)
ENDIF ()

ADD_EXECUTABLE (clickhouse-test-runner-ut
    ut/args_ut.cpp
    ut/format_ut.cpp
    ut/hint_ut.cpp
    ut/live_ut.cpp
    ut/render_ut.cpp
    ut/split_ut.cpp
)
TARGET_INCLUDE_DIRECTORIES (clickhouse-test-runner-ut PRIVATE
    ${PROJECT_SOURCE_DIR}/contrib/gtest/include
)
TARGET_LINK_LIBRARIES (clickhouse-test-runner-ut clickhouse-test-runner-lib gtest-lib)
