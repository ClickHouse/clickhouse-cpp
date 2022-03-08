ADD_EXECUTABLE (simple-test
    ../../ut/utils.cpp
    main.cpp
)

TARGET_LINK_LIBRARIES (simple-test
    clickhouse-cpp-lib-static
)

IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # there is a problem with __builtin_mul_overflow call at link time
    # the error looks like: ... undefined reference to `__muloti4' ... 
    # caused by clang bug https://bugs.llvm.org/show_bug.cgi?id=16404
    # explicit linking to compiler-rt allows to workaround the problem
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --rtlib=compiler-rt")
ENDIF ()
