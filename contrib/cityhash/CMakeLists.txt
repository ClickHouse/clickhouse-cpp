ADD_LIBRARY (cityhash-lib STATIC
    city.cc
)

set_property(TARGET cityhash-lib PROPERTY POSITION_INDEPENDENT_CODE ON)

INSTALL (TARGETS cityhash-lib EXPORT clickhouse-cpp-lib-staticConfig
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/clickhouse-cpp-deps)

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/"
  DESTINATION "include/clickhouse-cpp-deps/cityhash"
  FILES_MATCHING
  PATTERN "*.h"
  )
