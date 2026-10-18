#pragma once

#include <algorithm>
#include <cstdint>
#include <string>

namespace clickhouse {

class InputStream;
class OutputStream;

class WireFormat {
public:
    template <typename T>
    static bool ReadFixed(InputStream& input, T* value);
    static bool ReadString(InputStream& input, std::string* value);
    static bool SkipString(InputStream& input);
    static bool ReadBytes(InputStream& input, void* buf, size_t len);

    /// Replaces the contents of `container` with `count` elements read from `input`.
    /// The container grows with the data instead of being sized to `count` up front, so
    /// a count declared by the peer is never allocated before its bytes are read.
    template <typename Container>
    static bool ReadPODArray(InputStream& input, Container* container, size_t count);

    /// Capacity to grow to once `loaded` of `count` elements are held: eight times `loaded`
    /// and at least `first`, capped at `count`, without overflowing `Size`.
    template <typename Size>
    static constexpr Size NextReadCapacity(Size loaded, Size count, Size first) {
        return loaded > count / 8 ? count : std::min(count, std::max(first, static_cast<Size>(loaded * 8)));
    }

    static bool ReadUInt64(InputStream& input, uint64_t* value);
    static bool ReadVarint64(InputStream& output, uint64_t* value);

    template <typename T>
    static void WriteFixed(OutputStream& output, const T& value);
    static void WriteBytes(OutputStream& output, const void* buf, size_t len);
    static void WriteString(OutputStream& output, std::string_view value);
    static void WriteQuotedString(OutputStream& output, std::string_view value);
    static void WriteParamNullRepresentation(OutputStream& output);
    static void WriteUInt64(OutputStream& output, const uint64_t value);
    static void WriteVarint64(OutputStream& output, uint64_t value);

private:
    static bool ReadAll(InputStream& input, void* buf, size_t len);
    static void WriteAll(OutputStream& output, const void* buf, size_t len);
};

template <typename T>
inline bool WireFormat::ReadFixed(InputStream& input, T* value) {
    return ReadAll(input, value, sizeof(T));
}

inline bool WireFormat::ReadString(InputStream& input, std::string* value) {
    uint64_t len = 0;
    if (ReadVarint64(input, &len)) {
        if (len > 0x00FFFFFFULL) {
            return false;
        }
        return ReadPODArray(input, value, (size_t)len);
    }

    return false;
}

inline bool WireFormat::ReadBytes(InputStream& input, void* buf, size_t len) {
    return ReadAll(input, buf, len);
}

template <typename Container>
inline bool WireFormat::ReadPODArray(InputStream& input, Container* container, size_t count) {
    using T                      = typename Container::value_type;
    constexpr size_t first_chunk = std::max<size_t>(1, (1 << 20) / sizeof(T));

    container->clear();
    size_t loaded = 0;
    while (loaded < count) {
        // Reserving far ahead keeps reallocation copies small; resizing only by what is
        // about to be read keeps the zero-filled part proportional to the data received.
        if (container->capacity() <= loaded) {
            container->reserve(NextReadCapacity(loaded, count, first_chunk));
        }
        const size_t chunk = std::min({count - loaded, container->capacity() - loaded, std::max(loaded, first_chunk)});
        container->resize(loaded + chunk);
        if (!ReadAll(input, container->data() + loaded, chunk * sizeof(T))) {
            return false;
        }
        loaded += chunk;
    }

    return true;
}

inline bool WireFormat::ReadUInt64(InputStream& input, uint64_t* value) {
    return ReadVarint64(input, value);
}

template <typename T>
inline void WireFormat::WriteFixed(OutputStream& output, const T& value) {
    WriteAll(output, &value, sizeof(T));
}

inline void WireFormat::WriteBytes(OutputStream& output, const void* buf, size_t len) {
    WriteAll(output, buf, len);
}

inline void WireFormat::WriteString(OutputStream& output, std::string_view value) {
    WriteVarint64(output, value.size());
    WriteAll(output, value.data(), value.size());
}

inline void WireFormat::WriteUInt64(OutputStream& output, const uint64_t value) {
    WriteVarint64(output, value);
}

}
