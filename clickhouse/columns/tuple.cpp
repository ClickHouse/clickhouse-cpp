#include "tuple.h"

namespace clickhouse {

ColumnTuple::ColumnTuple(const std::vector<ColumnRef>& columns)
    : columns_(columns)
{
}

TypeRef ColumnTuple::Type() const {
    return type_;
}

size_t ColumnTuple::Size() const {
    return columns_.empty() ? 0 : columns_[0]->Size();
}

bool ColumnTuple::Load(CodedInputStream* input, size_t rows) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        if (!(*ci)->Load(input, rows)) {
            return false;
        }
    }

    return true;
}

void ColumnTuple::Save(CodedOutputStream* output) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        (*ci)->Save(output);
    }
}

}
