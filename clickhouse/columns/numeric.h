#pragma once

#include "clickhouse/columns/column.h"
#include "clickhouse/columns/utils.h"

namespace clickhouse {

/**
 * Represents various numeric columns.
 */
template <typename T>
class ColumnVector : public Column {
public:
    ColumnVector()
        : Column(Type::CreateSimple<T>())
    {
    }

    explicit ColumnVector(const std::vector<T>& data)
        : Column(Type::CreateSimple<T>())
        , data_(data)
    {
    }

    /// Appends one element to the end of column.
    void Append(const T& value) {
        data_.push_back(value);
    }

    /// Returns element at given row number.
    const T& At(size_t n) const {
        return data_.at(n);
    }

    /// Returns element at given row number.
    const T& operator [] (size_t n) const {
        return data_[n];
    }

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override {
        if (auto col = column->As<ColumnVector<T>>()) {
            data_.insert(data_.end(), col->data_.begin(), col->data_.end());
        }
    }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override {
        data_.resize(rows);

        return input->ReadRaw(data_.data(), data_.size() * sizeof(T));
    }

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override {
        output->WriteRaw(data_.data(), data_.size() * sizeof(T));
    }

    /// Returns count of rows in the column.
    size_t Size() const override {
        return data_.size();
    }

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override {
        return std::make_shared<ColumnVector<T>>(SliceVector(data_, begin, len));
    }

private:
    std::vector<T> data_;
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
