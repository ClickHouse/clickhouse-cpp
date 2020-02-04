#pragma once

#include "coded.h"

#include <string>

namespace clickhouse {

class WireFormat {
public:
    template <typename T>
    static bool ReadFixed(CodedInputStream* input, T* value);

    static bool ReadString(CodedInputStream* input, std::string* value);

    static bool ReadBytes(CodedInputStream* input, void* buf, size_t len);

    static bool ReadUInt64(CodedInputStream* input, uint64_t* value);


    template <typename T>
    static void WriteFixed(CodedOutputStream* output, const T& value);

    static void WriteBytes(CodedOutputStream* output, const void* buf, size_t len);

    static void WriteString(CodedOutputStream* output, std::string_view value);

    static void WriteUInt64(CodedOutputStream* output, const uint64_t value);
};

template <typename T>
inline bool WireFormat::ReadFixed(
    CodedInputStream* input,
    T* value)
{
    return input->ReadRaw(value, sizeof(T));
}

inline bool WireFormat::ReadString(
    CodedInputStream* input,
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
    CodedInputStream* input, void* buf, size_t len)
{
    return input->ReadRaw(buf, len);
}

inline bool WireFormat::ReadUInt64(
    CodedInputStream* input,
    uint64_t* value)
{
    return input->ReadVarint64(value);
}


template <typename T>
inline void WireFormat::WriteFixed(
    CodedOutputStream* output,
    const T& value)
{
    output->WriteRaw(&value, sizeof(T));
}

inline void WireFormat::WriteBytes(
    CodedOutputStream* output,
    const void* buf,
    size_t len)
{
    output->WriteRaw(buf, len);
}

inline void WireFormat::WriteString(
    CodedOutputStream* output,
    std::string_view value)
{
    output->WriteVarint64(value.size());
    output->WriteRaw(value.data(), value.size());
}

inline void WireFormat::WriteUInt64(
    CodedOutputStream* output,
    const uint64_t value)
{
    output->WriteVarint64(value);
}

}
