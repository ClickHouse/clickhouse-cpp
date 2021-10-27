#include "compressed.h"
#include "wire_format.h"
#include "output.h"

#include <cityhash/city.h>
#include <lz4/lz4.h>
#include <stdexcept>
#include <system_error>

#include <iostream>

namespace {
static const size_t HEADER_SIZE = 9;
static const size_t EXTRA_PREALLOCATE_COMPRESS_BUFFER = 15;
static const uint8_t COMPRESSION_METHOD = 0x82;
#define DBMS_MAX_COMPRESSED_SIZE    0x40000000ULL   // 1GB
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
            throw std::runtime_error("some data was not readed");
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

    if (!WireFormat::ReadFixed(input_, &hash)) {
        return false;
    }
    if (!WireFormat::ReadFixed(input_, &method)) {
        return false;
    }

    if (method != COMPRESSION_METHOD) {
        throw std::runtime_error("unsupported compression method " +
                                 std::to_string(int(method)));
    } else {
        if (!WireFormat::ReadFixed(input_, &compressed)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input_, &original)) {
            return false;
        }

        if (compressed > DBMS_MAX_COMPRESSED_SIZE) {
            throw std::runtime_error("compressed data too big");
        }

        Buffer tmp(compressed);

        // Заполнить заголовок сжатых данных.
        {
            BufferOutput out(&tmp);
            out.Write(&method,     sizeof(method));
            out.Write(&compressed, sizeof(compressed));
            out.Write(&original,   sizeof(original));
        }

        if (!WireFormat::ReadBytes(input_, tmp.data() + HEADER_SIZE, compressed - HEADER_SIZE)) {
            return false;
        } else {
            if (hash != CityHash128((const char*)tmp.data(), compressed)) {
                throw std::runtime_error("data was corrupted");
            }
        }

        data_ = Buffer(original);

        if (LZ4_decompress_safe((const char*)tmp.data() + HEADER_SIZE, (char*)data_.data(), compressed - HEADER_SIZE, original) < 0) {
            throw std::runtime_error("can't decompress data");
        } else {
            mem_.Reset(data_.data(), original);
        }
    }

    return true;
}


CompressedOutput::CompressedOutput(OutputStream * destination, size_t max_compressed_chunk_size)
    : destination_(destination),
      max_compressed_chunk_size_(max_compressed_chunk_size)
{
}

CompressedOutput::~CompressedOutput() {
        Flush();
}

size_t CompressedOutput::DoWrite(const void* data, size_t len) {
    const size_t original_len = len;
    const size_t max_chunk_size = max_compressed_chunk_size_ ? max_compressed_chunk_size_ : len;

    while (len > 0)
    {
        auto to_compress = std::min(len, max_chunk_size);
        if (!Compress(data, to_compress))
            break;

        len -= to_compress;
        data = reinterpret_cast<const char*>(data) + to_compress;
    }

    return original_len - len;
}

void CompressedOutput::DoFlush() {
    destination_->Flush();
}

bool CompressedOutput::Compress(const void * data, size_t len) {

    const size_t expected_out_size = LZ4_compressBound(len);
    compressed_buffer_.resize(std::max(compressed_buffer_.size(), expected_out_size + HEADER_SIZE + EXTRA_PREALLOCATE_COMPRESS_BUFFER));

    const int compressed_size = LZ4_compress_default(
            (const char*)data,
            (char*)compressed_buffer_.data() + HEADER_SIZE,
            len,
            compressed_buffer_.size() - HEADER_SIZE);

    {
        auto header = compressed_buffer_.data();
        WriteUnaligned(header, COMPRESSION_METHOD);
        // Compressed data size with header
        WriteUnaligned(header + 1, static_cast<uint32_t>(compressed_size + HEADER_SIZE));
        // Original data size
        WriteUnaligned(header + 5, static_cast<uint32_t>(len));
    }

    WireFormat::WriteFixed(destination_, CityHash128(
        (const char*)compressed_buffer_.data(), compressed_size + HEADER_SIZE));
    WireFormat::WriteBytes(destination_, compressed_buffer_.data(), compressed_size + HEADER_SIZE);

    destination_->Flush();
    return true;
}

}
