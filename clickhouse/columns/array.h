#pragma once

#include "clickhouse/columns/numeric.h"

namespace clickhouse {

/** */
class ColumnArray : public Column {
public:
    ColumnArray(ColumnRef data);

    /// Converts input column to array and appends
    /// as one row to the current column.
    void AppendAsColumn(ColumnRef array);

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef) override { }

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }

    /// Convets array at pos n to column.
    /// Element's type of result column same as type of array element.
    ColumnRef GetAsColumn(size_t n) const;

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

}
