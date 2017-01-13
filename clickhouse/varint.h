#pragma once

#include "io/coded_input.h"

#include <string>
#include <memory.h>

namespace clickhouse {

class WireFormat {
public:
    template <typename T>
    static bool ReadFixed(io::CodedInputStream* input, T* value);

    static bool ReadString(io::CodedInputStream* input, std::string* value);

    static bool ReadBytes(io::CodedInputStream* input, void* buf, size_t len);

    static bool ReadUInt64(io::CodedInputStream* input, uint64_t* value);
};

template <typename T>
inline bool WireFormat::ReadFixed(
    io::CodedInputStream* input,
    T* value)
{
    return input->ReadRaw(value, sizeof(T));
}

inline bool WireFormat::ReadString(
    io::CodedInputStream* input,
    std::string* value)
{
    uint64_t len;

    if (input->ReadVarint64(&len)) {
        if (len > 0x00FFFFFFULL) {
            return false;
        }
        value->resize((size_t)len);
        return input->ReadRaw(&(*value)[0], (size_t)len);
    }

    return false;
}

inline bool WireFormat::ReadBytes(
    io::CodedInputStream* input, void* buf, size_t len)
{
    return input->ReadRaw(buf, len);
}

inline bool WireFormat::ReadUInt64(
    io::CodedInputStream* input,
    uint64_t* value)
{
    return input->ReadVarint64(value);
}


inline char* writeVarUInt(uint64_t x, char* ostr) {
    for (size_t i = 0; i < 9; ++i) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;

        *ostr = byte;
        ++ostr;

        x >>= 7;
        if (!x)
            return ostr;
    }

    return ostr;
}

inline char* writeStringBinary(const std::string& s, char* ostr) {
    ostr = writeVarUInt(s.size(), ostr);
    memcpy(ostr, s.data(), s.size());
    ostr += s.size();
    return ostr;
}

template <typename T>
inline char* writeBinary(const T& val, char* ostr) {
    memcpy(ostr, &val, sizeof(T));
    ostr += sizeof(T);
    return ostr;
}

}
