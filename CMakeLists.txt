CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/cpp17.cmake)
INCLUDE (cmake/subdirs.cmake)

OPTION(BUILD_BENCHMARK "Build benchmark" OFF)

PROJECT (CLICKHOUSE-CLIENT)

    USE_CXX17()

    IF (UNIX)
        IF (APPLE)
            SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Wall -Wextra -Werror")
        ELSE ()
            SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -pthread -Wall -Wextra -Werror")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "${CMAKE_EXE_LINKER_FLAGS} -lpthread")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)
    INCLUDE_DIRECTORIES(contrib)

    SUBDIRS (
        clickhouse
        contrib/cityhash
        contrib/gtest
        contrib/lz4
        tests/simple
        ut
    )

    IF (BUILD_BENCHMARK)
        SUBDIRS(bench)
    ENDIF (BUILD_BENCHMARK)
