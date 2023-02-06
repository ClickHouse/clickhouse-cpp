CMAKE_MINIMUM_REQUIRED (VERSION 3.0.2)

LIST (APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

INCLUDE (cpp17)
INCLUDE (subdirs)
INCLUDE (openssl)

OPTION (BUILD_BENCHMARK "Build benchmark" OFF)
OPTION (BUILD_TESTS "Build tests" OFF)
OPTION (BUILD_SHARED_LIBS "Build shared libs" OFF)
OPTION (WITH_OPENSSL "Use OpenSSL for TLS connections" OFF)
OPTION (WITH_SYSTEM_ABSEIL "Use system ABSEIL" OFF)
OPTION (WITH_SYSTEM_LZ4 "Use system LZ4" OFF)
OPTION (WITH_SYSTEM_CITYHASH "Use system cityhash" OFF)

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

    IF (APPLE OR MSVC)
        IF(BUILD_SHARED_LIBS)
            MESSAGE(FATAL "Does not support shared on this platform")
        ENDIF()
    ENDIF()

    IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        INCLUDE (CheckCXXSourceCompiles)

        CHECK_CXX_SOURCE_COMPILES("#include <bits/c++config.h>\nint main() { return __GLIBCXX__ != 0; }"
                                  CLANG_WITH_LIB_STDCXX)
    ENDIF ()

    IF (CLANG_WITH_LIB_STDCXX)
        # there is a problem with __builtin_mul_overflow call at link time
        # the error looks like: ... undefined reference to `__muloti4' ...
        # caused by clang bug https://bugs.llvm.org/show_bug.cgi?id=16404
        # explicit linking to compiler-rt allows to workaround the problem
        SET (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --rtlib=compiler-rt")
        SET (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --rtlib=compiler-rt")

        # some workaround for linking issues on linux:
        # /usr/bin/ld: CMakeFiles/simple-test.dir/main.cpp.o: undefined reference to symbol '_Unwind_Resume@@GCC_3.0'
        # /usr/bin/ld: /lib/x86_64-linux-gnu/libgcc_s.so.1: error adding symbols: DSO missing from command line
        # FIXME: that workaround breaks clang build on mingw
        SET (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -lgcc_s")
        SET (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcc_s")
    ENDIF ()

    IF (WITH_SYSTEM_ABSEIL)
        FIND_PACKAGE(absl REQUIRED)
    ELSE ()
        INCLUDE_DIRECTORIES (contrib/absl)
        SUBDIRS (contrib/absl/absl)
    ENDIF ()

    IF (WITH_SYSTEM_LZ4)
        FIND_PACKAGE(lz4 REQUIRED)
    ELSE ()
        INCLUDE_DIRECTORIES (contrib/lz4/lz4)
        SUBDIRS (contrib/lz4/lz4)
    ENDIF ()

    IF (WITH_SYSTEM_CITYHASH)
        FIND_PACKAGE(cityhash REQUIRED)
    ELSE ()
        INCLUDE_DIRECTORIES (contrib/cityhash/cityhash)
        SUBDIRS (contrib/cityhash/cityhash)
    ENDIF ()

    SUBDIRS (
        clickhouse
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
