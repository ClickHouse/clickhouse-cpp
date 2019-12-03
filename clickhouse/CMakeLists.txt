SET ( clickhouse-cpp-lib-src
    base/coded.cpp
    base/compressed.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp

    columns/array.cpp
    columns/date.cpp
    columns/decimal.cpp
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

ADD_LIBRARY (clickhouse-cpp-lib SHARED ${clickhouse-cpp-lib-src})

SET_TARGET_PROPERTIES(clickhouse-cpp-lib
    PROPERTIES LINKER_LANGUAGE CXX)

TARGET_LINK_LIBRARIES (clickhouse-cpp-lib
    cityhash-lib
    lz4-lib
)

ADD_LIBRARY (clickhouse-cpp-lib-static STATIC ${clickhouse-cpp-lib-src})
TARGET_LINK_LIBRARIES (clickhouse-cpp-lib-static
    cityhash-lib
    lz4-lib
)

INSTALL(TARGETS clickhouse-cpp-lib LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
INSTALL(TARGETS clickhouse-cpp-lib-static LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# general
INSTALL(FILES block.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/)
INSTALL(FILES client.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/)
INSTALL(FILES error_codes.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/)
INSTALL(FILES exceptions.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/)
INSTALL(FILES protocol.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/)
INSTALL(FILES query.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/)

# base
INSTALL(FILES base/buffer.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/coded.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/compressed.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/input.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/output.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/platform.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/singleton.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/socket.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/string_utils.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/string_view.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)
INSTALL(FILES base/wire_format.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/base/)

# columns
INSTALL(FILES columns/array.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/column.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/date.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/decimal.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/enum.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/factory.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/nullable.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/numeric.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/string.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/tuple.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/utils.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)
INSTALL(FILES columns/uuid.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/columns/)

# types
INSTALL(FILES types/type_parser.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/types/)
INSTALL(FILES types/types.h DESTINATION ${INCLUDE_INSTALL_DIR}/clickhouse/types/)

