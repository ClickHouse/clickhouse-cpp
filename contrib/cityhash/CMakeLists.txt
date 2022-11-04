ADD_LIBRARY (cityhash-lib STATIC
    city.cc
)

set_property(TARGET cityhash-lib PROPERTY POSITION_INDEPENDENT_CODE ON)

INSTALL (TARGETS cityhash-lib EXPORT clickhouse-cpp-lib-staticConfig
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/clickhouse-cpp-deps)
