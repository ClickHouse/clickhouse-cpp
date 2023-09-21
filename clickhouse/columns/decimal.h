#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

/**
 * Represents a column of decimal type.
 */
class ColumnDecimal : public Column {
public:
    using ValueType = Int128;

    ColumnDecimal(size_t precision, size_t scale);

    void Append(const Int128& value);
    void Append(const std::string& value);

    Int128 At(size_t i) const;
    inline auto operator[](size_t i) const { return At(i); }

    /// Swap two Elements/rows in the column
    void SwapElements(size_t pos1, size_t pos2);

    /// Test if the value at position 1 is greater than the value at position 2
    /// No range checking is performed for performance
    bool CompareElementsGT(size_t pos1, size_t pos2) const;

    /// Test if the value at position 1 is less than the value at position 2
    /// No range checking is performed for performance
    bool CompareElementsLT(size_t pos1, size_t pos2) const;

public:
    void Append(ColumnRef column) override;
    bool LoadBody(InputStream* input, size_t rows) override;
    void SaveBody(OutputStream* output) override;
    void Clear() override;
    size_t Size() const override;

    /// Increase the capacity of the column
    void Reserve(size_t new_cap) override;

    /// Returns the capacity of the column
    size_t Capacity() const override;

    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t index) const override;

    size_t GetScale() const;
    size_t GetPrecision() const;

private:
    /// Depending on a precision it can be one of:
    ///  - ColumnInt32
    ///  - ColumnInt64
    ///  - ColumnInt128
    ColumnRef data_;

    explicit ColumnDecimal(TypeRef type, ColumnRef data);
};

}
