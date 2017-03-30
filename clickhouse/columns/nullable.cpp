#include "nullable.h"

#include <assert.h>

namespace clickhouse {

ColumnNullable::ColumnNullable(TypeRef nested)
    : Column(Type::CreateNullable(nested))
    , nulls_(std::make_shared<ColumnUInt8>())
{
}

ColumnNullable::ColumnNullable(ColumnRef nested, ColumnRef nulls)
    : Column(Type::CreateNullable(nested->Type()))
    , nested_(nested)
    , nulls_(nulls->As<ColumnUInt8>())
{
    if (nested_->Size() != nulls->Size()) {
        throw std::runtime_error("count of elements in nested and nulls should be the same");
    }
}

/// Returns null flag at given row number.
bool ColumnNullable::IsNull(size_t n) const {
    return nulls_->At(n) != 0;
}

/// Returns nested column.
ColumnRef ColumnNullable::Nested() const {
    return nested_;
}

void ColumnNullable::Append(ColumnRef column) {
    (void)column;
}

bool ColumnNullable::Load(CodedInputStream* input, size_t rows) {
    if (!nulls_->Load(input, rows)) {
        return false;
    }
    if (!nested_->Load(input, rows)) {
        return false;
    }
    return true;
}

void ColumnNullable::Save(CodedOutputStream* output) {
    nulls_->Save(output);
    nested_->Save(output);
}

size_t ColumnNullable::Size() const {
    assert(nested_->Size() == nulls_->Size());
    return nulls_->Size();
}

ColumnRef ColumnNullable::Slice(size_t begin, size_t len) {
    (void)begin;
    (void)len;
    return ColumnRef();
}

}
