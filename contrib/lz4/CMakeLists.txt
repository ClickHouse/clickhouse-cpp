ADD_LIBRARY (lz4-lib STATIC
    lz4.c
    lz4hc.c
)

set_property(TARGET lz4-lib PROPERTY POSITION_INDEPENDENT_CODE ON)
