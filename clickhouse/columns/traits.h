#pragma once

namespace clickhouse {

template <typename T>
struct TypeToColumn;

template <> struct TypeToColumn<int8_t>   { using Column = ColumnInt8; };
template <> struct TypeToColumn<int16_t>  { using Column = ColumnInt16; };
template <> struct TypeToColumn<int32_t>  { using Column = ColumnInt32; };
template <> struct TypeToColumn<int64_t>  { using Column = ColumnInt64; };

template <> struct TypeToColumn<uint8_t>  { using Column = ColumnUInt8; };
template <> struct TypeToColumn<uint16_t> { using Column = ColumnUInt16; };
template <> struct TypeToColumn<uint32_t> { using Column = ColumnUInt32; };
template <> struct TypeToColumn<uint64_t> { using Column = ColumnUInt64; };

}
