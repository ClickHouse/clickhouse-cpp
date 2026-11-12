#include "compressed.h"
#include "wire_format.h"
#include "output.h"
#include "clickhouse/exceptions.h"

#include <city.h>
#include <lz4.h>
#include <exception>
#include <zstd.h>
#include <stdexcept>
#include <system_error>

namespace {
constexpr size_t HEADER_SIZE = 9;

// see DB::CompressionMethodByte from src/Compression/CompressionInfo.h of ClickHouse project
enum class CompressionMethodByte : uint8_t {
    NONE = 0x02,
    LZ4  = 0x82,
    ZSTD = 0x90,
};

// Documentation says that compression is faster when output buffer is larger than LZ4_compressBound/ZSTD_compressBound estimation.
constexpr size_t EXTRA_COMPRESS_BUFFER_SIZE = 4096;
constexpr size_t DBMS_MAX_COMPRESSED_SIZE = 0x40000000ULL;   // 1GB
// The same limits as ClickHouse applies to the sizes in a block header (src/Compression/CompressionInfo.h).
constexpr size_t DBMS_MAX_DECOMPRESSED_SIZE = 0x40000000ULL;  // 1GB
// The compressed payload is read in steps starting at this size, so that a declared
// size is not allocated before the bytes behind it arrive. It covers the server's
// default 1 MiB blocks even when they do not compress.
constexpr size_t COMPRESSED_READ_STEP = 4 << 20;
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
            throw CompressionError("some data was not read");
        }
    }
}

size_t CompressedInput::DoNext(const void** ptr, size_t len) {
    while (mem_.Exhausted()) {
        if (!Decompress()) {
            return 0;
        }
    }

    return mem_.Next(ptr, len);
}

bool CompressedInput::Decompress() {
    cityhash::uint128 hash;
    uint32_t compressed = 0;
    uint32_t original = 0;
    uint8_t method = 0;

    if (!WireFormat::ReadFixed(*input_, &hash)) {
        return false;
    }
    if (!WireFormat::ReadFixed(*input_, &method)) {
        return false;
    }

    if (method != static_cast<uint8_t>(CompressionMethodByte::LZ4) && method != static_cast<uint8_t>(CompressionMethodByte::ZSTD)) {
        throw CompressionError("unsupported compression method " + std::to_string((method)));
    }

    if (!WireFormat::ReadFixed(*input_, &compressed)) {
        return false;
    }
    if (!WireFormat::ReadFixed(*input_, &original)) {
        return false;
    }

    if (compressed > DBMS_MAX_COMPRESSED_SIZE) {
        throw CompressionError("compressed block size " + std::to_string(compressed) + " exceeds maximum allowed, data is likely corrupted");
    }

    if (compressed < HEADER_SIZE) {
        throw CompressionError("compressed block size " + std::to_string(compressed) + " is smaller than header, data is likely corrupted");
    }

    if (original > DBMS_MAX_DECOMPRESSED_SIZE) {
        throw CompressionError("decompressed block size " + std::to_string(original) +
                               " exceeds maximum allowed, data is likely corrupted");
    }

    Buffer tmp;

    // Data header
    {
        BufferOutput out(&tmp);
        out.Write(&method, sizeof(method));
        out.Write(&compressed, sizeof(compressed));
        out.Write(&original, sizeof(original));
        out.Flush();
    }

    while (tmp.size() < compressed) {
        const size_t loaded = tmp.size();
        if (tmp.capacity() <= loaded) {
            // Eightfold, without overflowing a 32-bit size_t near the 1 GiB limit.
            const size_t target = loaded > compressed / 8 ? compressed : std::max(COMPRESSED_READ_STEP, loaded * 8);
            tmp.reserve(std::min<size_t>(compressed, target));
        }
        const size_t step = std::min({compressed - loaded, tmp.capacity() - loaded, std::max(loaded, COMPRESSED_READ_STEP)});
        tmp.resize(loaded + step);
        if (!WireFormat::ReadBytes(*input_, tmp.data() + loaded, step)) {
            return false;
        }
    }

    if (hash != cityhash::CityHash128((const char*)tmp.data(), compressed)) {
        throw CompressionError("data was corrupted");
    }

    // Left uninitialized: zero-filling would commit the whole declared size even when
    // the payload decompresses to far less, which the length checks below reject.
    if (original > data_capacity_) {
        data_.reset(new uint8_t[original]);
        data_capacity_ = original;
    }

    // data_ is null until a non-empty block arrives, and the decompressors do
    // pointer arithmetic on the destination even when its capacity is zero.
    char empty_output  = 0;
    char* const output = original > 0 ? reinterpret_cast<char*>(data_.get()) : &empty_output;

    switch (method) {
    case static_cast<uint8_t>(CompressionMethodByte::LZ4): {
        const int res =
            LZ4_decompress_safe((const char*)tmp.data() + HEADER_SIZE, output, static_cast<int>(compressed - HEADER_SIZE), original);
        if (res < 0) {
            throw CompressionError("can't decompress LZ4-encoded data");
        }
        if (static_cast<uint32_t>(res) != original) {
            throw CompressionError("LZ4-encoded block decompressed to " + std::to_string(res) + " bytes, expected " +
                                   std::to_string(original));
        }
        mem_.Reset(data_.get(), original);
        return true;
    }

    case static_cast<uint8_t>(CompressionMethodByte::ZSTD): {
        size_t res = ZSTD_decompress(output, original, (const char*)tmp.data() + HEADER_SIZE, static_cast<int>(compressed - HEADER_SIZE));

        if (ZSTD_isError(res)) {
            throw CompressionError("can't decompress ZSTD-encoded data, ZSTD error: " + std::string(ZSTD_getErrorName(res)));
        }
        if (res != original) {
            throw CompressionError("ZSTD-encoded block decompressed to " + std::to_string(res) + " bytes, expected " +
                                   std::to_string(original));
        }
        mem_.Reset(data_.get(), original);
        return true;
    }

    case static_cast<uint8_t>(CompressionMethodByte::NONE): {
        throw CompressionError("compression method not defined" + std::to_string((method)));
    }
    default: {
        throw CompressionError("Unknown or unsupported compression method " + std::to_string((method)));
    }
    }

    return true;
}


CompressedOutput::CompressedOutput(OutputStream * destination, size_t max_compressed_chunk_size, CompressionMethod method)
    : destination_(destination)
    , max_compressed_chunk_size_(max_compressed_chunk_size)
    , method_(method)
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
    switch (method_) {  
    case clickhouse::CompressionMethod::LZ4: {
        const auto compressed_size = LZ4_compress_default(
                (const char*)data,
                (char*)compressed_buffer_.data() + HEADER_SIZE,
                static_cast<int>(len),
                static_cast<int>(compressed_buffer_.size() - HEADER_SIZE));
        if (compressed_size <= 0)
            throw CompressionError("Failed to compress chunk of " + std::to_string(len) + " bytes, "
                    "LZ4 error: " + std::to_string(compressed_size));

        {
            auto header = compressed_buffer_.data();
            WriteUnaligned(header, CompressionMethodByte::LZ4);
            // Compressed data size with header
            WriteUnaligned(header + 1, static_cast<uint32_t>(compressed_size + HEADER_SIZE));
            // Original data size
            WriteUnaligned(header + 5, static_cast<uint32_t>(len));
        }

        WireFormat::WriteFixed(*destination_, cityhash::CityHash128((const char*)compressed_buffer_.data(), compressed_size + HEADER_SIZE));
        WireFormat::WriteBytes(*destination_, compressed_buffer_.data(), compressed_size + HEADER_SIZE);
        break;
    }

    case clickhouse::CompressionMethod::ZSTD: {
        const size_t compressed_size = ZSTD_compress(
                (char*)compressed_buffer_.data() + HEADER_SIZE,
                static_cast<int>(compressed_buffer_.size() - HEADER_SIZE),
                (const char*)data,
                static_cast<int>(len),
                ZSTD_fast);
        if (ZSTD_isError(compressed_size))
            throw CompressionError("Failed to compress chunk of " + std::to_string(len) + " bytes, "
                    "ZSTD error: " + std::string(ZSTD_getErrorName(compressed_size)));

        {
            auto header = compressed_buffer_.data();
            WriteUnaligned(header, CompressionMethodByte::ZSTD);
            // Compressed data size with header
            WriteUnaligned(header + 1, static_cast<uint32_t>(compressed_size + HEADER_SIZE));
            // Original data size
            WriteUnaligned(header + 5, static_cast<uint32_t>(len));
        }

        WireFormat::WriteFixed(*destination_, cityhash::CityHash128((const char*)compressed_buffer_.data(), compressed_size + HEADER_SIZE));
        WireFormat::WriteBytes(*destination_, compressed_buffer_.data(), compressed_size + HEADER_SIZE);
        break;
    }

    case clickhouse::CompressionMethod::None: {
        throw CompressionError("no compression defined");
    }
    }

    destination_->Flush();
}

void CompressedOutput::PreallocateCompressBuffer(size_t input_size) {
    switch (method_) {  
    case clickhouse::CompressionMethod::LZ4: {
        const auto estimated_compressed_buffer_size = LZ4_compressBound(static_cast<int>(input_size));
        if (estimated_compressed_buffer_size <= 0)
            throw CompressionError("Failed to estimate compressed buffer size, LZ4 error: " + std::to_string(estimated_compressed_buffer_size));

        compressed_buffer_.resize(estimated_compressed_buffer_size + HEADER_SIZE + EXTRA_COMPRESS_BUFFER_SIZE);
        break;
    }

    case clickhouse::CompressionMethod::ZSTD: {
        const size_t estimated_compressed_buffer_size = ZSTD_compressBound(static_cast<int>(input_size));
        if (ZSTD_isError(estimated_compressed_buffer_size))
            throw CompressionError("Failed to estimate compressed buffer size, ZSTD error: " + std::string(ZSTD_getErrorName(estimated_compressed_buffer_size)));

        compressed_buffer_.resize(estimated_compressed_buffer_size + HEADER_SIZE + EXTRA_COMPRESS_BUFFER_SIZE);
        break;
    }

    case clickhouse::CompressionMethod::None: {
        /// do nothing
        break;
    }
    }
}

}
