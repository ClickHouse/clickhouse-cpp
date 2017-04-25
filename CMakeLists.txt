CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/subdirs.cmake)
INCLUDE (cmake/cpp11.cmake)

PROJECT (CLICKHOUSE-CLIENT)

    USE_CXX11()

    IF (UNIX)
        IF (APPLE)
            SET (CMAKE_CXX_FLAGS "-Wall -Wextra -Werror")
        ELSE ()
            SET (CMAKE_CXX_FLAGS "-pthread -Wall -Wextra -Werror")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "-lpthread")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)
    INCLUDE_DIRECTORIES(contrib/cityhash)
    INCLUDE_DIRECTORIES(contrib/lz4)

    SUBDIRS (
        clickhouse
        contrib/cityhash
        contrib/gtest
        contrib/lz4
        tests/simple
        ut
    )
