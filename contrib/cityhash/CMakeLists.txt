ADD_LIBRARY (cityhash-lib STATIC
    city.cc
)

set_property(TARGET cityhash-lib PROPERTY POSITION_INDEPENDENT_CODE ON)
