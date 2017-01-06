#include "coded_input.h"

#include <memory.h>

namespace clickhouse {
namespace io {

CodedInputStream::CodedInputStream(ZeroCopyInput* input)
    : input_(input)
    , buffer_(nullptr)
    , buffer_end_(nullptr)
{
}

CodedInputStream::CodedInputStream(const uint8_t* buffer, size_t size)
    : input_(nullptr)
    , buffer_(buffer)
    , buffer_end_(buffer + size)
{
}

bool CodedInputStream::ReadRaw(void* buffer, size_t size) {
    uint8_t* p = static_cast<uint8_t*>(buffer);

    while (size > 0) {
        const void* ptr;
        size_t len = input_->Next(&ptr, size);

        memcpy(p, ptr, len);

        p += len;
        size -= len;
    }

    return true;
}

bool CodedInputStream::ReadVarint64(uint64_t* value) {
    *value = 0;

    for (size_t i = 0; i < 9; ++i) {
        uint8_t byte;

        if (!input_->ReadByte(&byte)) {
            return false;
        } else {
            *value |= (byte & 0x7F) << (7 * i);

            if (!(byte & 0x80)) {
                return true;
            }
        }
    }

    // TODO skip invalid
    return false;
}

}
}
