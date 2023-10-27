ADD_EXECUTABLE (simple-test
    ../../ut/utils.cpp
    main.cpp
)

TARGET_LINK_LIBRARIES (simple-test
    clickhouse-cpp-lib
)

IF (MSVC)
    TARGET_LINK_LIBRARIES (simple-test Crypt32)
ENDIF()