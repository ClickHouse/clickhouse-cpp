#include "uuid.h"
#include "utils.h"

#include <stdexcept>

namespace clickhouse {

ColumnUUID::ColumnUUID()
    : Column(Type::CreateUUID())
    , data_(std::make_shared<ColumnUInt64>())
{
}

ColumnUUID::ColumnUUID(ColumnRef data)
    : Column(Type::CreateUUID())
    , data_(data->As<ColumnUInt64>())
{
    if (data_->Size() % 2 != 0) {
        throw std::runtime_error("number of entries must be even (two 64-bit numbers for each UUID)");
    }
}

void ColumnUUID::Append(const UInt128& value) {
    data_->Append(value.first);
    data_->Append(value.second);
}

void ColumnUUID::Clear() {
    data_->Clear();
}

const UInt128 ColumnUUID::At(size_t n) const {
    return UInt128(data_->At(n * 2), data_->At(n * 2 + 1));
}

const UInt128 ColumnUUID::operator [] (size_t n) const {
    return UInt128((*data_)[n * 2], (*data_)[n * 2 + 1]);
}

void ColumnUUID::Append(ColumnRef column) {
    if (auto col = column->As<ColumnUUID>()) {
        data_->Append(col->data_);
    }
}

bool ColumnUUID::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows * 2);
}

void ColumnUUID::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnUUID::Size() const {
    return data_->Size() / 2;
}

ColumnRef ColumnUUID::Slice(size_t begin, size_t len) {
    return std::make_shared<ColumnUUID>(data_->Slice(begin * 2, len * 2));
}

void ColumnUUID::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnUUID &>(other);
    data_.swap(col.data_);
}

ItemView ColumnUUID::GetItem(size_t index) const {
    return data_->GetItem(index);
}

}

