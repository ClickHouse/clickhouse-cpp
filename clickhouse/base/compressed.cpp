#include "compressed.h"
#include "wire_format.h"

#include <cityhash/city.h>
#include <lz4/lz4.h>
#include <stdexcept>
#include <system_error>

#define DBMS_MAX_COMPRESSED_SIZE    0x40000000ULL   // 1GB

namespace clickhouse {

CompressedInput::CompressedInput(CodedInputStream* input)
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

    if (method != 0x82) {
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

        if (!WireFormat::ReadBytes(input_, tmp.data() + 9, compressed - 9)) {
            return false;
        } else {
            if (hash != CityHash128((const char*)tmp.data(), compressed)) {
                throw std::runtime_error("data was corrupted");
            }
        }

        data_ = Buffer(original);

        if (LZ4_decompress_fast((const char*)tmp.data() + 9, (char*)data_.data(), original) < 0) {
            throw std::runtime_error("can't decompress data");
        } else {
            mem_.Reset(data_.data(), original);
        }
    }

    return true;
}

}
