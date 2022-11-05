ADD_LIBRARY (absl-lib STATIC
    numeric/int128.cc
)

INSTALL (TARGETS absl-lib EXPORT clickhouse-cpp-lib-staticConfig
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/clickhouse-cpp-deps
  )

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/"
  DESTINATION "include/clickhouse-cpp-deps/absl"
  FILES_MATCHING
  PATTERN "*.h"
  )
