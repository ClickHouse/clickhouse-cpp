#pragma once

#include "query.h"

#include <stdexcept>

namespace clickhouse {

class ServerException : public std::runtime_error {
public:
    ServerException(std::unique_ptr<Exception> e)
        : runtime_error(std::string())
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

}
