#include "map.h"

#include <stdexcept>

#include "../exceptions.h"
#include "utils.h"

namespace {

using namespace clickhouse;

TypeRef GetMapType(const Type& data_type) {
    auto array = data_type.As<ArrayType>();
    if (!array) {
        throw ValidationError("Wrong type  " + data_type.GetName() + " of data for map");
    }
    auto tuple = array->GetItemType()->As<TupleType>();
    if (!tuple) {
        throw ValidationError("Wrong type  " + data_type.GetName() + " of data for map");
    }
    auto types = tuple->GetTupleType();
    if (types.size() != 2) {
        throw ValidationError("Wrong type  " + data_type.GetName() + " of data for map");
    }
    return Type::CreateMap(types[0], types[1]);
}

}  // namespace

namespace clickhouse {

ColumnMap::ColumnMap(ColumnRef data)
    : Column(GetMapType(data->GetType()), Serialization::MakeDefault(this))
    , data_(data->As<ColumnArray>()) {
}

void ColumnMap::Clear() {
    data_->Clear();
}

void ColumnMap::Append(ColumnRef column) {
    if (auto col = column->As<ColumnMap>()) {
        data_->Append(col->data_);
    }
}

bool ColumnMap::LoadPrefix(InputStream* input, size_t rows) {
    return data_->GetSerialization()->LoadPrefix(data_.get(), input, rows);
}

bool ColumnMap::LoadBody(InputStream* input, size_t rows) {
    return data_->GetSerialization()->LoadBody(data_.get(),input, rows);
}

void ColumnMap::SavePrefix(OutputStream* output) {
    data_->GetSerialization()->SavePrefix(data_.get(), output);
}

void ColumnMap::SaveBody(OutputStream* output) {
    data_->GetSerialization()->SaveBody(data_.get(), output);
}

size_t ColumnMap::Size() const {
    return data_->Size();
}

ColumnRef ColumnMap::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnMap>(data_->Slice(begin, len));
}

ColumnRef ColumnMap::CloneEmpty() const {
    return std::make_shared<ColumnMap>(data_->CloneEmpty());
}

void ColumnMap::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnMap&>(other);
    data_.swap(col.data_);
}

void ColumnMap::SetSerializationKind(Serialization::Kind kind) {
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

ColumnRef ColumnMap::GetAsColumn(size_t n) const {
    return data_->GetAsColumn(n);
}

}  // namespace clickhouse
