#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

/**
 * Represents column of Array(T).
 */
class ColumnArray : public Column {
public:
    ColumnArray(ColumnRef data);

    /// Converts input column to array and appends
    /// as one row to the current column.
    void AppendAsColumn(ColumnRef array);

    /// Convets array at pos n to column.
    /// Type of element of result column same as type of array element.
    ColumnRef GetAsColumn(size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream.
    void SavePrefix(OutputStream* output) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t, size_t) const override;

    void Swap(Column&) override;

    void OffsetsIncrease(size_t);

private:
    size_t GetOffset(size_t n) const;

    size_t GetSize(size_t n) const;

private:
    ColumnRef data_;
    std::shared_ptr<ColumnUInt64> offsets_;
};

}
