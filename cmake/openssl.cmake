MACRO (USE_OPENSSL)

    if (WITH_OPENSSL)
        find_package(OpenSSL REQUIRED)
        message("Found OpenSSL version: ${OPENSSL_VERSION} at ${OPENSSL_INCLUDE_DIR}")
        add_compile_definitions(WITH_OPENSSL=1)
    endif()

ENDMACRO(USE_OPENSSL)
