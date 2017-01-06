CMAKE_MINIMUM_REQUIRED(VERSION 3.1)

INCLUDE (cmake/subdirs.cmake)

PROJECT (CLICKHOUSE-CLIENT)

    SET (CMAKE_CXX_STANDARD 11)
    SET (CMAKE_CXX_STANDARD_REQUIRED ON)

    IF (UNIX)
        SET (CMAKE_CXX_FLAGS "-pthread")
        SET (CMAKE_EXE_LINKER_FLAGS, "-lpthread")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)

    SUBDIRS (
        clickhouse
        tests/simple
    )
