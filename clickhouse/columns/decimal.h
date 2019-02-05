#pragma once

#include "column.h"

namespace clickhouse {

using Int128 = __int128;

/**
 * Represents a column of decimal type.
 */

class ColumnDecimal : public Column {
public:
    ColumnDecimal(size_t precision, size_t scale);

    void Append(const Int128& value);
    void Append(const std::string& value);

    Int128 At(size_t i) const;

public:
    void Append(ColumnRef column) override { data_->Append(column); }
    bool Load(CodedInputStream* input, size_t rows) override { return data_->Load(input, rows); }
    void Save(CodedOutputStream* output) override { data_->Save(output); }
    void Clear() override { data_->Clear(); }
    size_t Size() const override { return data_->Size(); }
    ColumnRef Slice(size_t begin, size_t len) override;

private:
    /// Depending on a precision it can be one of:
    ///  - ColumnInt32
    ///  - ColumnInt64
    ///  - ColumnInt128
    ColumnRef data_;

    explicit ColumnDecimal(TypeRef type); // for `Slice(…)`
};

}  // namespace clickhouse
