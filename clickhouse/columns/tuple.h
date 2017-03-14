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

    TypeRef Type() const override;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    std::vector<ColumnRef> columns_;
    TypeRef type_;
};

}
