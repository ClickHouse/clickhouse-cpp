#pragma once

#include "input.h"
#include "output.h"
#include "buffer.h"

namespace clickhouse {

class CompressedInput : public ZeroCopyInput {
public:
     CompressedInput(InputStream* input);
    ~CompressedInput();

protected:
    size_t DoNext(const void** ptr, size_t len) override;

    bool Decompress();

private:
    InputStream* const input_;

    Buffer data_;
    ArrayInput mem_;
};

class CompressedOutput : public OutputStream {
public:
    CompressedOutput(OutputStream * destination, size_t max_compressed_chunk_size = 0);
    ~CompressedOutput();

protected:
    size_t DoWrite(const void* data, size_t len) override;
    void DoFlush() override;
    bool Compress(const void * data, size_t len);


private:
    OutputStream * destination_;
    Buffer compressed_buffer_;
    size_t max_compressed_chunk_size_;
};

}
