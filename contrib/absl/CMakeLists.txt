ADD_LIBRARY (absl-lib STATIC
    numeric/int128.cc
)

INSTALL (TARGETS absl-lib EXPORT clickhouse-cpp-lib-staticConfig
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/clickhouse-cpp-deps)
