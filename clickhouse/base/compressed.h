#pragma once

#include "coded.h"

namespace clickhouse {

class CompressedInput : public ZeroCopyInput {
public:
     CompressedInput(CodedInputStream* input);
    ~CompressedInput();

protected:
    size_t DoNext(const void** ptr, size_t len) override;

    bool Decompress();

private:
    CodedInputStream* const input_;

    Buffer data_;
    ArrayInput mem_;
};

}
