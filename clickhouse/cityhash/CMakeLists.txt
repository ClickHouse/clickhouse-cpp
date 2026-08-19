ADD_LIBRARY (cityhash STATIC
    city.cc
)

set_property(TARGET cityhash PROPERTY POSITION_INDEPENDENT_CODE ON)

ADD_LIBRARY (cityhash::cityhash ALIAS cityhash)
