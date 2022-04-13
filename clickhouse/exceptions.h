#pragma once

#include "server_exception.h"

#include <stdexcept>

namespace clickhouse {

class Error : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

class ValidationError : public Error {
    using Error::Error;
};

class ProtocolError : public Error {
    using Error::Error;
};

class UnimplementedError : public Error {
    using Error::Error;
};

class AssertionError : public Error {
    using Error::Error;
};

class OpenSSLError : public Error {
    using Error::Error;
};

class LZ4Error : public Error {
    using Error::Error;
};

class ServerException : public Error {
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
