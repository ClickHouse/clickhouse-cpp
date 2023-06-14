#include "compressed.h"
#include "wire_format.h"
#include "output.h"
#include "../exceptions.h"

#include <city.h>
#include <lz4.h>
#include <stdexcept>
#include <system_error>

namespace {
constexpr size_t HEADER_SIZE = 9;
// see DB::CompressionMethodByte::LZ4 from src/Compression/CompressionInfo.h of ClickHouse project
constexpr uint8_t COMPRESSION_METHOD = 0x82;
// Documentation says that compression is faster when output buffer is larger than LZ4_compressBound estimation.
constexpr size_t EXTRA_COMPRESS_BUFFER_SIZE = 4096;
constexpr size_t DBMS_MAX_COMPRESSED_SIZE = 0x40000000ULL;   // 1GB
}

namespace clickhouse {

CompressedInput::CompressedInput(InputStream* input)
    : input_(input)
{
}

CompressedInput::~CompressedInput() {
    if (!mem_.Exhausted()) {
#if __cplusplus < 201703L
        if (!std::uncaught_exception()) {
#else
        if (!std::uncaught_exceptions()) {
#endif
            throw LZ4Error("some data was not read");
        }
    }
}

size_t CompressedInput::DoNext(const void** ptr, size_t len) {
    if (mem_.Exhausted()) {
        if (!Decompress()) {
            return 0;
        }
    }

    return mem_.Next(ptr, len);
}

bool CompressedInput::Decompress() {
    uint128 hash;
    uint32_t compressed = 0;
    uint32_t original = 0;
    uint8_t method = 0;

    if (!WireFormat::ReadFixed(*input_, &hash)) {
        return false;
    }
    if (!WireFormat::ReadFixed(*input_, &method)) {
        return false;
    }

    if (method != COMPRESSION_METHOD) {
        throw LZ4Error("unsupported compression method " + std::to_string(int(method)));
    } else {
        if (!WireFormat::ReadFixed(*input_, &compressed)) {
            return false;
        }
        if (!WireFormat::ReadFixed(*input_, &original)) {
            return false;
        }

        if (compressed > DBMS_MAX_COMPRESSED_SIZE) {
            throw LZ4Error("compressed data too big");
        }

        Buffer tmp(compressed);

        // Data header
        {
            BufferOutput out(&tmp);
            out.Write(&method,     sizeof(method));
            out.Write(&compressed, sizeof(compressed));
            out.Write(&original,   sizeof(original));
            out.Flush();
        }

        if (!WireFormat::ReadBytes(*input_, tmp.data() + HEADER_SIZE, compressed - HEADER_SIZE)) {
            return false;
        } else {
            if (hash != CityHash128((const char*)tmp.data(), compressed)) {
                throw LZ4Error("data was corrupted");
            }
        }

        data_ = Buffer(original);

        if (LZ4_decompress_safe((const char*)tmp.data() + HEADER_SIZE, (char*)data_.data(), static_cast<int>(compressed - HEADER_SIZE), original) < 0) {
            throw LZ4Error("can't decompress data");
        } else {
            mem_.Reset(data_.data(), original);
        }
    }

    return true;
}


CompressedOutput::CompressedOutput(OutputStream * destination, size_t max_compressed_chunk_size)
    : destination_(destination)
    , max_compressed_chunk_size_(max_compressed_chunk_size)
{
    PreallocateCompressBuffer(max_compressed_chunk_size);
}

CompressedOutput::~CompressedOutput() { }

size_t CompressedOutput::DoWrite(const void* data, size_t len) {
    const size_t original_len = len;
    // what if len > max_compressed_chunk_size_ ?
    const size_t max_chunk_size = max_compressed_chunk_size_ > 0 ? max_compressed_chunk_size_ : len;
    if (max_chunk_size > max_compressed_chunk_size_) {
        PreallocateCompressBuffer(len);
    }

    while (len > 0) {
        auto to_compress = std::min(len, max_chunk_size);
        Compress(data, to_compress);

        len -= to_compress;
        data = reinterpret_cast<const char*>(data) + to_compress;
    }

    return original_len - len;
}

void CompressedOutput::DoFlush() {
    destination_->Flush();
}

void CompressedOutput::Compress(const void * data, size_t len) {
    const auto compressed_size = LZ4_compress_default(
            (const char*)data,
            (char*)compressed_buffer_.data() + HEADER_SIZE,
            static_cast<int>(len),
            static_cast<int>(compressed_buffer_.size() - HEADER_SIZE));
    if (compressed_size <= 0)
        throw LZ4Error("Failed to compress chunk of " + std::to_string(len) + " bytes, "
                "LZ4 error: " + std::to_string(compressed_size));

    {
        auto header = compressed_buffer_.data();
        WriteUnaligned(header, COMPRESSION_METHOD);
        // Compressed data size with header
        WriteUnaligned(header + 1, static_cast<uint32_t>(compressed_size + HEADER_SIZE));
        // Original data size
        WriteUnaligned(header + 5, static_cast<uint32_t>(len));
    }

    WireFormat::WriteFixed(*destination_, CityHash128(
        (const char*)compressed_buffer_.data(), compressed_size + HEADER_SIZE));
    WireFormat::WriteBytes(*destination_, compressed_buffer_.data(), compressed_size + HEADER_SIZE);

    destination_->Flush();
}

void CompressedOutput::PreallocateCompressBuffer(size_t input_size) {
    const auto estimated_compressed_buffer_size = LZ4_compressBound(static_cast<int>(input_size));
    if (estimated_compressed_buffer_size <= 0)
        throw LZ4Error("Failed to estimate compressed buffer size, LZ4 error: " + std::to_string(estimated_compressed_buffer_size));

    compressed_buffer_.resize(estimated_compressed_buffer_size + HEADER_SIZE + EXTRA_COMPRESS_BUFFER_SIZE);
}

}
