#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

/**
 * Represents column of Nullable(T).
 */
class ColumnNullable : public Column {
public:
    ColumnNullable(ColumnRef nested, ColumnRef nulls);

    /// Appends one null flag to the end of the column
    void Append(bool isnull);

    /// Returns null flag at given row number.
    bool IsNull(size_t n) const;

    /// Returns nested column.
    ColumnRef Nested() const;

    /// Returns nulls column.
    ColumnRef Nulls() const;

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
    void Swap(Column&) override;

    ItemView GetItem(size_t) const override;

private:
    ColumnRef nested_;
    std::shared_ptr<ColumnUInt8> nulls_;
};

}
