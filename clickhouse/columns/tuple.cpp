#include "tuple.h"

namespace clickhouse {

static std::vector<TypeRef> CollectTypes(const std::vector<ColumnRef>& columns) {
    std::vector<TypeRef> types;
    for (const auto& col : columns) {
        types.push_back(col->Type());
    }
    return types;
}

ColumnTuple::ColumnTuple(const std::vector<ColumnRef>& columns)
    : Column(Type::CreateTuple(CollectTypes(columns)))
    , columns_(columns)
{
}

size_t ColumnTuple::TupleSize() const {
    return columns_.size();
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

void ColumnTuple::Clear() {
    columns_.clear();
}

void ColumnTuple::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnTuple &>(other);
    columns_.swap(col.columns_);
}

}
