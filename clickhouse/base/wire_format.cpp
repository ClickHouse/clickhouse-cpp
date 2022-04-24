#include "wire_format.h"

#include "input.h"
#include "output.h"

#include "../exceptions.h"

#include <stdexcept>

namespace {
constexpr int MAX_VARINT_BYTES = 10;
}

namespace clickhouse {

bool WireFormat::ReadAll(InputStream& input, void* buf, size_t len) {
    uint8_t* p = static_cast<uint8_t*>(buf);

    size_t read_previously = 1; // 1 to execute loop at least once
    while (len > 0 && read_previously) {
        read_previously = input.Read(p, len);

        p += read_previously;
        len -= read_previously;
    }

    return !len;
}

void WireFormat::WriteAll(OutputStream& output, const void* buf, size_t len) {
    const size_t original_len = len;
    const uint8_t* p = static_cast<const uint8_t*>(buf);

    size_t written_previously = 1; // 1 to execute loop at least once
    while (len > 0 && written_previously) {
        written_previously = output.Write(p, len);

        p += written_previously;
        len -= written_previously;
    }

    if (len) {
        throw ProtocolError("Failed to write " + std::to_string(original_len)
                + " bytes, only written " + std::to_string(original_len - len));
    }
}

bool WireFormat::ReadVarint64(InputStream& input, uint64_t* value) {
    *value = 0;

    for (size_t i = 0; i < MAX_VARINT_BYTES; ++i) {
        uint8_t byte = 0;

        if (!input.ReadByte(&byte)) {
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

void WireFormat::WriteVarint64(OutputStream& output, uint64_t value) {
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

    WriteAll(output, bytes, size);
}

bool WireFormat::SkipString(InputStream& input) {
    uint64_t len = 0;

    if (ReadVarint64(input, &len)) {
        if (len > 0x00FFFFFFULL)
            return false;

        return input.Skip((size_t)len);
    }

    return false;
}

}
