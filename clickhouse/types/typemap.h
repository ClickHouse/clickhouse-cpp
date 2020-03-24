#pragma once

#include "../types/types.h"

#include <string>
#include <stirng_view>

namespace clickhouse {

template <typename T>
Type::Code GetTypeCode() {
    if constexpr (std::is_same_v<std::string, T> || std::is_same_v<std::string_view, T>) {
        return Type::Code::String;
    } else if constexpr (std::is_signed_v<T> && sizeof(T) == sizeof(std::int8_t)) {
        return Type::Code::Int8;
    } else if constexpr (std::is_unsigned_v<T> && sizeof(T) == sizeof(std::uint8_t)) {
        return Type::Code::UInt8;
    } else if constexpr (std::is_signed_v<T> && sizeof(T) == sizeof(std::int16_t)) {
        return Type::Code::Int16;
    } else if constexpr (std::is_unsigned_v<T> && sizeof(T) == sizeof(std::uint16_t)) {
        return Type::Code::UInt16;
    } else if constexpr (std::is_signed_v<T> && sizeof(T) == sizeof(std::uint32_t)) {
        return Type::Code::Int32;
    } else if constexpr (std::is_unsigned_v<T> && sizeof(T) == sizeof(std::int32_t)) {
        return Type::Code::UInt32;
    } else if constexpr (std::is_signed_v<T> && sizeof(T) == sizeof(std::int64_t)) {
        return Type::Code::Int64;
    } else if constexpr (std::is_unsigned_v<T> && sizeof(T) == sizeof(std::uint64_t)) {
        return Type::Code::UInt64;
    }
}

}
