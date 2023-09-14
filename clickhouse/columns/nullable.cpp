#include "nullable.h"

#include <assert.h>
#include <stdexcept>

namespace clickhouse {

ColumnNullable::ColumnNullable(ColumnRef nested, ColumnRef nulls)
    : Column(Type::CreateNullable(nested->Type()), Serialization::MakeDefault(this))
    , nested_(nested)
    , nulls_(nulls->As<ColumnUInt8>())
{
    if (nested_->Size() != nulls->Size()) {
        throw ValidationError("count of elements in nested and nulls should be the same");
    }
}

void ColumnNullable::Append(bool isnull)
{
    nulls_->Append(isnull ? 1 : 0);
}


bool ColumnNullable::IsNull(size_t n) const {
    return nulls_->At(n) != 0;
}

ColumnRef ColumnNullable::Nested() const {
    return nested_;
}

ColumnRef ColumnNullable::Nulls() const
{
       return nulls_;
}

void ColumnNullable::Append(ColumnRef column) {
    if (auto col = column->As<ColumnNullable>()) {
        if (!col->nested_->Type()->IsEqual(nested_->Type())) {
            return;
        }

        nested_->Append(col->nested_);
        nulls_->Append(col->nulls_);
    }
}

void ColumnNullable::Clear() {
    nested_->Clear();
    nulls_->Clear();
}

bool ColumnNullable::LoadPrefix(InputStream* input, size_t rows) {
    return nested_->GetSerialization()->LoadPrefix(nested_.get(), input, rows);
}

bool ColumnNullable::LoadBody(InputStream* input, size_t rows) {
    if (!nulls_->GetSerialization()->LoadBody(nulls_.get(), input, rows)) {
        return false;
    }
    if (!nested_->GetSerialization()->LoadBody(nested_.get(), input, rows)) {
        return false;
    }
    return true;
}

void ColumnNullable::SavePrefix(OutputStream* output) {
    nested_->GetSerialization()->SavePrefix(nested_.get(), output);
}

void ColumnNullable::SaveBody(OutputStream* output) {
    nulls_->GetSerialization()->SaveBody(nulls_.get(), output);
    nested_->GetSerialization()->SaveBody(nested_.get(), output);
}

size_t ColumnNullable::Size() const {
    return nulls_->Size();
}

ColumnRef ColumnNullable::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnNullable>(nested_->Slice(begin, len), nulls_->Slice(begin, len));
}

ColumnRef ColumnNullable::CloneEmpty() const {
    return std::make_shared<ColumnNullable>(nested_->CloneEmpty(), nulls_->CloneEmpty());
}

void ColumnNullable::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnNullable &>(other);
    if (!nested_->Type()->IsEqual(col.nested_->Type()))
        throw ValidationError("Can't swap() Nullable columns of different types.");

    nested_.swap(col.nested_);
    nulls_.swap(col.nulls_);
}

ItemView ColumnNullable::GetItem(size_t index) const  {
    if (IsNull(index))
        return ItemView();

    return nested_->GetItem(index);
}

void ColumnNullable::SetSerializationKind(Serialization::Kind kind) {
    switch (kind)
    {
    case Serialization::Kind::DEFAULT:
        serialization_ = Serialization::MakeDefault(this);
        break;
    default:
        throw UnimplementedError("Serialization kind:" + std::to_string(static_cast<int>(kind))
            + " is not supported for column of " + type_->GetName());
    }
}

}
