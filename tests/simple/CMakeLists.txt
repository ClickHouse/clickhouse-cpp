ADD_EXECUTABLE (simple-test
	main.cpp
)

OPTION(BUILD_SAMPLE_WITH_INT128 "Build sample program with Int128 support" ON)

TARGET_LINK_LIBRARIES (simple-test
	clickhouse-cpp-lib
)

if (BUILD_TESTS_WITH_INT128)
    target_compile_definitions(simple-test WITH_INT128)
endif()
