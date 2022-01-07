ADD_EXECUTABLE (simple-test
	main.cpp
)

TARGET_LINK_LIBRARIES (simple-test
	clickhouse-cpp-lib
)

IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --rtlib=compiler-rt")
ENDIF ()
