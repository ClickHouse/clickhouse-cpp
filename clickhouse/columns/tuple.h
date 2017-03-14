#pragma once

#include "clickhouse/columns/column.h"

#include <vector>

namespace clickhouse {

/** */
class ColumnTuple : public Column {
public:
    ColumnTuple(const std::vector<ColumnRef>& columns);

    ColumnRef operator [] (size_t n) const {
        return columns_[n];
    }

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef) override { }

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }

private:
    std::vector<ColumnRef> columns_;
};

}
