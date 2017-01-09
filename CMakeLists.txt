CMAKE_MINIMUM_REQUIRED(VERSION 3.1)

INCLUDE (cmake/subdirs.cmake)

PROJECT (CLICKHOUSE-CLIENT)

    SET (CMAKE_CXX_STANDARD 11)
    SET (CMAKE_CXX_STANDARD_REQUIRED ON)

    IF (UNIX)
        IF (APPLE)
            SET (CMAKE_CXX_FLAGS "-Wall -Wextra -Werror")
        ELSE ()
            SET (CMAKE_CXX_FLAGS "-pthread -Wall -Wextra -Werror")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "-lpthread")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)

    SUBDIRS (
        clickhouse
        tests/simple
    )
