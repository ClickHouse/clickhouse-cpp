#include "coded.h"

#include <memory.h>

namespace clickhouse {

static const int MAX_VARINT_BYTES = 10;

CodedInputStream::CodedInputStream(ZeroCopyInput* input)
    : input_(input)
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

bool CodedInputStream::Skip(size_t count) {
    while (count > 0) {
        const void* ptr;
        size_t len = input_->Next(&ptr, count);

        if (len == 0) {
            return false;
        }

        count -= len;
    }

    return true;
}

bool CodedInputStream::ReadVarint64(uint64_t* value) {
    *value = 0;

    for (size_t i = 0; i < MAX_VARINT_BYTES; ++i) {
        uint8_t byte;

        if (!input_->ReadByte(&byte)) {
            return false;
        } else {
            *value |= uint64_t(byte & 0x7F) << (7 * i);

            if (!(byte & 0x80)) {
                return true;
            }
        }
    }

    // TODO skip invalid
    return false;
}


CodedOutputStream::CodedOutputStream(ZeroCopyOutput* output)
    : output_(output)
{
}

void CodedOutputStream::Flush() {
    output_->Flush();
}

void CodedOutputStream::WriteRaw(const void* buffer, int size) {
    output_->Write(buffer, size);
}

void CodedOutputStream::WriteVarint64(uint64_t value) {
    uint8_t bytes[MAX_VARINT_BYTES];
    int size = 0;

    for (size_t i = 0; i < MAX_VARINT_BYTES; ++i) {
        uint8_t byte = value & 0x7F;
        if (value > 0x7F)
            byte |= 0x80;

        bytes[size++] = byte;

        value >>= 7;
        if (!value) {
            break;
        }
    }

    WriteRaw(bytes, size);
}

}
