#pragma once

#include "server_exception.h"

#include <stdexcept>

namespace clickhouse {

class Error : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Caused by any user-related code, like invalid column types or arguments passed to any method.
class ValidationError final : public Error {
    using Error::Error;
};

// Buffers+IO errors, failure to serialize/deserialize, checksum mismatches, etc.
class ProtocolError final : public Error {
    using Error::Error;
};

class UnimplementedError final : public Error {
    using Error::Error;
};

// Internal validation error.
class AssertionError final : public Error {
    using Error::Error;
};

class OpenSSLError final : public Error {
    using Error::Error;
};

class LZ4Error final : public Error {
    using Error::Error;
};

// Exception received from server.
class ServerException final : public Error {
public:
    ServerException(std::unique_ptr<Exception> e)
        : Error(std::string())
        , exception_(std::move(e))
    {
    }

    int GetCode() const {
        return exception_->code;
    }

    const Exception& GetException() const {
        return *exception_;
    }

    const char* what() const noexcept override {
        return exception_->display_text.c_str();
    }

private:
    std::unique_ptr<Exception> exception_;
};
using ServerError = ServerException;

}
