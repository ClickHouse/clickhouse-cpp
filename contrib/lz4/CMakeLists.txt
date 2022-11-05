ADD_LIBRARY (lz4-lib STATIC
    lz4.c
    lz4hc.c
)

set_property(TARGET lz4-lib PROPERTY POSITION_INDEPENDENT_CODE ON)

INSTALL (TARGETS lz4-lib EXPORT clickhouse-cpp-lib-staticConfig
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/clickhouse-cpp-deps)

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/"
  DESTINATION "include/clickhouse-cpp-deps/lz4"
  FILES_MATCHING
  PATTERN "*.h"
  )
