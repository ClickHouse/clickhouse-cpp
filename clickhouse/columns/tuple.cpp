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

void ColumnTuple::Append(ColumnRef column) {
    if (!this->Type()->IsEqual(column->Type())) {
        throw std::runtime_error(
            "can't append column of type " + column->Type()->GetName() + " "
            "to column type " + this->Type()->GetName());
    }
    const auto & source_tuple_column = column->As<ColumnTuple>();
    for (size_t ci = 0; ci < columns_.size(); ++ci) {
        columns_[ci]->Append((*source_tuple_column)[ci]);
    }
}
size_t ColumnTuple::Size() const {
    return columns_.empty() ? 0 : columns_[0]->Size();
}
ColumnRef ColumnTuple::Slice(size_t begin, size_t len) const {
    std::vector<ColumnRef> sliced_columns;
    sliced_columns.reserve(columns_.size());
    for(const auto &column : columns_){
        sliced_columns.push_back(column->Slice(begin, len));
    }

    return std::make_shared<ColumnTuple>(sliced_columns);
}

bool ColumnTuple::Load(InputStream* input, size_t rows) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        if (!(*ci)->Load(input, rows)) {
            return false;
        }
    }

    return true;
}

void ColumnTuple::Save(OutputStream* output) {
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
