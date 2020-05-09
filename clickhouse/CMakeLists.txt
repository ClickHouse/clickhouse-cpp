ADD_LIBRARY (clickhouse-cpp-lib
    base/coded.cpp
    base/compressed.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp

    columns/array.cpp
    columns/date.cpp
    columns/enum.cpp
    columns/factory.cpp
    columns/nullable.cpp
    columns/numeric.cpp
    columns/string.cpp
    columns/tuple.cpp
    columns/uuid.cpp

    types/type_parser.cpp
    types/types.cpp

    block.cpp
    client.cpp
    query.cpp
)

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)

TARGET_LINK_LIBRARIES (clickhouse-cpp-lib
    cityhash-lib
    lz4-lib
)


INSTALL(TARGETS clickhouse-cpp-lib
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        )




# general
INSTALL(FILES block.h DESTINATION include/clickhouse/)
INSTALL(FILES client.h DESTINATION include/clickhouse/)
INSTALL(FILES error_codes.h DESTINATION include/clickhouse/)
INSTALL(FILES exceptions.h DESTINATION include/clickhouse/)
INSTALL(FILES protocol.h DESTINATION include/clickhouse/)
INSTALL(FILES query.h DESTINATION include/clickhouse/)

# base
INSTALL(FILES base/buffer.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/coded.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/compressed.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/input.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/output.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/platform.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/singleton.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/socket.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/string_utils.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/string_view.h DESTINATION include/clickhouse/base/)
INSTALL(FILES base/wire_format.h DESTINATION include/clickhouse/base/)

# columns
INSTALL(FILES columns/column.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/array.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/date.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/enum.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/factory.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/nullable.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/numeric.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/string.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/tuple.h DESTINATION include/clickhouse/columns/)
INSTALL(FILES columns/uuid.h DESTINATION include/clickhouse/columns/)

# types
INSTALL(FILES types/type_parser.h DESTINATION include/clickhouse/types/)
INSTALL(FILES types/types.h DESTINATION include/clickhouse/types/)