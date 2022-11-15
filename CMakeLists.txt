CMAKE_MINIMUM_REQUIRED (VERSION 3.0.2)

INCLUDE (cmake/cpp17.cmake)
INCLUDE (cmake/subdirs.cmake)
INCLUDE (cmake/openssl.cmake)

OPTION (BUILD_BENCHMARK "Build benchmark" OFF)
OPTION (BUILD_TESTS "Build tests" OFF)
OPTION (WITH_OPENSSL "Use OpenSSL for TLS connections" OFF)

PROJECT (CLICKHOUSE-CLIENT)

    USE_CXX17 ()
    USE_OPENSSL ()

    IF (NOT CMAKE_BUILD_TYPE)
        SET (CMAKE_BUILD_TYPE "RelWithDebInfo")
    ENDIF ()

    IF (UNIX)
        IF (NOT APPLE)
            SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "${CMAKE_EXE_LINKER_FLAGS} -lpthread")
        # -Wpedantic makes int128 support somewhat harder and less performant (by not allowing builtin __int128)
        # -Wno-deprecated-declarations to produce less cluttered output when building library itself (`deprecated` attributes are for library users)
        SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-deprecated-declarations")
    ENDIF ()

    SUBDIRS (
        clickhouse
        contrib/absl
        contrib/cityhash
        contrib/lz4
    )

    IF (BUILD_BENCHMARK)
        SUBDIRS (bench)
    ENDIF (BUILD_BENCHMARK)

    IF (BUILD_TESTS)
        INCLUDE_DIRECTORIES (contrib/gtest/include contrib/gtest)
        SUBDIRS (
            contrib/gtest
            tests/simple
            ut
        )
    ENDIF (BUILD_TESTS)
