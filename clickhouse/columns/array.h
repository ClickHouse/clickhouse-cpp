#pragma once

#include "clickhouse/columns/numeric.h"

namespace clickhouse {

template <typename T>
struct TypeToColumn;

/** */
class ColumnArray : public Column {
public:
    ColumnArray(ColumnRef data);

    TypeRef Type() const override;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    template <typename T>
    std::vector<int8_t> GetArray(size_t n) const {
        size_t offset = GetOffset(n);
        size_t size = GetSize(n);
        std::vector<int8_t> result;

        for (size_t i = offset; i < offset + size; ++i) {
            result.push_back((*data_->As<typename TypeToColumn<T>::Column>())[i]);
        }

        return result;
    }

private:
    inline size_t GetOffset(size_t n) const {
        return (n == 0) ? 0 : (*offsets_)[n - 1];
    }

    inline size_t GetSize(size_t n) const {
        return (n == 0) ? (*offsets_)[n] : ((*offsets_)[n] - (*offsets_)[n - 1]);
    }

private:
    ColumnRef data_;
    std::shared_ptr<ColumnUInt64> offsets_;
};

template <> struct TypeToColumn<int8_t>   { using Column = ColumnInt8; };
template <> struct TypeToColumn<int16_t>  { using Column = ColumnInt16; };
template <> struct TypeToColumn<int32_t>  { using Column = ColumnInt32; };
template <> struct TypeToColumn<int64_t>  { using Column = ColumnInt64; };

template <> struct TypeToColumn<uint8_t>  { using Column = ColumnUInt8; };
template <> struct TypeToColumn<uint16_t> { using Column = ColumnUInt16; };
template <> struct TypeToColumn<uint32_t> { using Column = ColumnUInt32; };
template <> struct TypeToColumn<uint64_t> { using Column = ColumnUInt64; };

}
