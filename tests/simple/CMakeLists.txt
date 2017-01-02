ADD_EXECUTABLE (simple-test
	main.cpp
)

TARGET_LINK_LIBRARIES (simple-test
	clickhouse-cpp-lib
)