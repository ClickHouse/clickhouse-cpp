#pragma once

#include "column.h"

namespace clickhouse {


template <typename T>
class ColumnEnum : public Column {
public:
    ColumnEnum(TypeRef type);
    ColumnEnum(TypeRef type, const std::vector<T>& data);

    /// Appends one element to the end of column.
    void Append(const T& value, bool checkValue = false);
    void Append(const std::string& name);

    /// Returns element at given row number.
    const T& At(size_t n) const;
    const std::string NameAt(size_t n) const;

    /// Returns element at given row number.
    const T& operator[] (size_t n) const;

    /// Set element at given row number.
    void SetAt(size_t n, const T& value, bool checkValue = false);
    void SetNameAt(size_t n, const std::string& name);

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;
    
    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

private:
    std::vector<T> data_;
};

using ColumnEnum8 = ColumnEnum<int8_t>;
using ColumnEnum16 = ColumnEnum<int16_t>;

}
