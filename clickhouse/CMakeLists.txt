SET ( clickhouse-cpp-lib-src
    base/compressed.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp
    base/wire_format.cpp

    columns/array.cpp
    columns/column.cpp
    columns/date.cpp
    columns/decimal.cpp
    columns/enum.cpp
    columns/factory.cpp
    columns/ip4.cpp
    columns/ip6.cpp
    columns/lowcardinality.cpp
    columns/lowcardinalityadaptor.h
    columns/nullable.cpp
    columns/numeric.cpp
    columns/string.cpp
    columns/tuple.cpp
    columns/uuid.cpp

    columns/itemview.cpp

    types/type_parser.cpp
    types/types.cpp

    block.cpp
    client.cpp
    query.cpp
)

IF (WITH_OPENSSL)
    LIST(APPEND clickhouse-cpp-lib-src base/sslsocket.cpp)
ENDIF ()

ADD_LIBRARY (clickhouse-cpp-lib SHARED ${clickhouse-cpp-lib-src})
SET_TARGET_PROPERTIES(clickhouse-cpp-lib PROPERTIES LINKER_LANGUAGE CXX)
TARGET_LINK_LIBRARIES (clickhouse-cpp-lib
    absl-lib
    cityhash-lib
    lz4-lib
)
TARGET_INCLUDE_DIRECTORIES (clickhouse-cpp-lib
    PUBLIC ${PROJECT_SOURCE_DIR}
)

ADD_LIBRARY (clickhouse-cpp-lib-static STATIC ${clickhouse-cpp-lib-src})
TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static
    absl-lib
    cityhash-lib
    lz4-lib
)
TARGET_INCLUDE_DIRECTORIES (clickhouse-cpp-lib-static
    PUBLIC ${PROJECT_SOURCE_DIR}
)

IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    INCLUDE (CheckCXXSourceCompiles)

    CHECK_CXX_SOURCE_COMPILES("#include <bits/c++config.h>\nint main() { return __GLIBCXX__ != 0; }"
                              BUILDING_WITH_LIB_STDCXX)

    IF (BUILDING_WITH_LIB_STDCXX)
        # there is a problem with __builtin_mul_overflow call at link time
        # the error looks like: ... undefined reference to `__muloti4' ...
        # caused by clang bug https://bugs.llvm.org/show_bug.cgi?id=16404
        # explicit linking to compiler-rt allows to workaround the problem
        SET (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --rtlib=compiler-rt")

        # some workaround for linking issues on linux:
        # /usr/bin/ld: CMakeFiles/simple-test.dir/main.cpp.o: undefined reference to symbol '_Unwind_Resume@@GCC_3.0'
        # /usr/bin/ld: /lib/x86_64-linux-gnu/libgcc_s.so.1: error adding symbols: DSO missing from command line
        # FIXME: that workaround breaks clang build on mingw
        TARGET_LINK_LIBRARIES (clickhouse-cpp-lib gcc_s)
        TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static gcc_s)
    ENDIF ()
ENDIF ()

INSTALL (TARGETS clickhouse-cpp-lib clickhouse-cpp-lib-static
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)


# general
INSTALL(FILES block.h DESTINATION include/clickhouse/)
INSTALL(FILES client.h DESTINATION include/clickhouse/)
INSTALL(FILES error_codes.h DESTINATION include/clickhouse/)
INSTALL(FILES exceptions.h DESTINATION include/clickhouse/)
INSTALL(FILES server_exception.h DESTINATION include/clickhouse/)
INSTALL(FILES protocol.h DESTINATION include/clickhouse/)
INSTALL(FILES query.h DESTINATION include/clickhouse/)

# base
INSTALL(FILES base/buffer.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/compressed.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/input.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/open_telemetry.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/output.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/platform.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/singleton.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/socket.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/string_utils.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/string_view.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/uuid.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/wire_format.h DESTINATION include/clickhouse/base/)

# columns
INSTALL(FILES columns/array.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/column.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/date.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/decimal.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/enum.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/factory.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/ip4.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/ip6.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/itemview.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/lowcardinality.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/nullable.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/numeric.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/string.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/tuple.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/utils.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/uuid.h DESTINATION include/clickhouse/columns/)

# types
INSTALL(FILES types/type_parser.h DESTINATION include/clickhouse/types/)
INSTALL(FILES types/types.h DESTINATION include/clickhouse/types/)

IF (WITH_OPENSSL)
    TARGET_LINK_LIBRARIES (clickhouse-cpp-lib OpenSSL::SSL)
    TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static OpenSSL::SSL)
ENDIF ()

IF (WIN32 OR MINGW)
    TARGET_LINK_LIBRARIES (clickhouse-cpp-lib wsock32 ws2_32)
    TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static wsock32 ws2_32)
ENDIF ()
