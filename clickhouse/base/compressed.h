#pragma once

#include "input.h"
#include "output.h"
#include "buffer.h"

namespace clickhouse {

/// Methods of block compression.
enum class CompressionMethod : int8_t {
    None = -1,
    LZ4  = 1,
    ZSTD = 2,
};

// see DB::CompressionMethodByte from src/Compression/CompressionInfo.h of ClickHouse project
enum class CompressionMethodByte : uint8_t {
    NONE = 0x02,
    LZ4  = 0x82,
    ZSTD = 0x90,
};

class CompressedInput : public ZeroCopyInput {
public:
    explicit CompressedInput(InputStream* input);
    ~CompressedInput() override;

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
    explicit CompressedOutput(OutputStream* destination, size_t max_compressed_chunk_size = 0, CompressionMethod method = CompressionMethod::LZ4);
    ~CompressedOutput() override;

protected:
    size_t DoWrite(const void* data, size_t len) override;
    void DoFlush() override;

private:
    void Compress(const void * data, size_t len);
    void PreallocateCompressBuffer(size_t input_size);

private:
    OutputStream * destination_;
    const size_t max_compressed_chunk_size_;
    Buffer compressed_buffer_;
    CompressionMethod method_;
};

}
