#pragma once

#include "clickhouse/columns/column.h"

namespace clickhouse {

/** */
template <typename T>
class ColumnVector : public Column {
public:
    /// Append one element to the column.
    void Append(const T& value) {
        data_.push_back(value);
    }

    const T& operator [] (size_t n) const {
        return data_[n];
    }

    TypeRef Type() const override {
        return type_;
    }

    size_t Size() const override {
        return data_.size();
    }

    bool Load(CodedInputStream* input, size_t rows) override {
        data_.resize(rows);

        return input->ReadRaw(data_.data(), data_.size() * sizeof(T));
    }

    void Save(CodedOutputStream* output) override {
        output->WriteRaw(data_.data(), data_.size() * sizeof(T));
    }

protected:
    std::vector<T> data_;
    TypeRef type_ = Type::CreateSimple<T>();
};

using ColumnUInt8   = ColumnVector<uint8_t>;
using ColumnUInt16  = ColumnVector<uint16_t>;
using ColumnUInt32  = ColumnVector<uint32_t>;
using ColumnUInt64  = ColumnVector<uint64_t>;

using ColumnInt8    = ColumnVector<int8_t>;
using ColumnInt16   = ColumnVector<int16_t>;
using ColumnInt32   = ColumnVector<int32_t>;
using ColumnInt64   = ColumnVector<int64_t>;

using ColumnFloat32 = ColumnVector<float>;
using ColumnFloat64 = ColumnVector<double>;

}
