#pragma once

#include "column.h"

#include <vector>

namespace clickhouse {

/**
 * Represents column of Tuple([T]).
 */
class ColumnTuple : public Column {
public:
    ColumnTuple(const std::vector<ColumnRef>& columns);

    ColumnRef operator [] (size_t n) const {
        return columns_[n];
    }

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef) override { }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;
    
    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }

private:
    std::vector<ColumnRef> columns_;
};

}
