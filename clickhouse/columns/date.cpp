#include "date.h"

namespace clickhouse {

ColumnDate::ColumnDate()
    : Column(Type::CreateDate())
    , data_(std::make_shared<ColumnUInt16>())
{
}

void ColumnDate::Append(const std::time_t& value) {
    /// TODO: This code is fundamentally wrong.
    data_->Append(static_cast<uint16_t>(value / std::time_t(86400)));
}

void ColumnDate::Clear() {
    data_->Clear();
}

std::time_t ColumnDate::At(size_t n) const {
    return static_cast<std::time_t>(data_->At(n)) * 86400;
}

void ColumnDate::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDate>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDate::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnDate::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnDate::Size() const {
    return data_->Size();
}

ColumnRef ColumnDate::Slice(size_t begin, size_t len) {
    auto col = data_->Slice(begin, len)->As<ColumnUInt16>();
    auto result = std::make_shared<ColumnDate>();

    result->data_->Append(col);

    return result;
}

void ColumnDate::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDate &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDate::GetItem(size_t index) const {
    return data_->GetItem(index);
}



ColumnDateTime::ColumnDateTime()
    : Column(Type::CreateDateTime())
    , data_(std::make_shared<ColumnUInt32>())
{
}

void ColumnDateTime::Append(const std::time_t& value) {
    data_->Append(static_cast<uint32_t>(value));
}

std::time_t ColumnDateTime::At(size_t n) const {
    return data_->At(n);
}

void ColumnDateTime::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDateTime>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDateTime::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnDateTime::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnDateTime::Size() const {
    return data_->Size();
}

void ColumnDateTime::Clear() {
    data_->Clear();
}

ColumnRef ColumnDateTime::Slice(size_t begin, size_t len) {
    auto col = data_->Slice(begin, len)->As<ColumnUInt32>();
    auto result = std::make_shared<ColumnDateTime>();

    result->data_->Append(col);

    return result;
}

void ColumnDateTime::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDateTime &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDateTime::GetItem(size_t index) const {
    return data_->GetItem(index);
}


ColumnDateTime64::ColumnDateTime64()
    : Column(Type::CreateDateTime64(3ul))
    , data_(std::make_shared<ColumnDecimal>(18ul, 3ul))
{}

ColumnDateTime64::ColumnDateTime64(size_t precision)
    : Column(Type::CreateDateTime64(precision))
    , data_(std::make_shared<ColumnDecimal>(18ul, precision))
{}


void ColumnDateTime64::Append(const Int128& value) {
    data_->Append(value);
}

void ColumnDateTime64::Append(const std::string& value) {
    data_->Append(value);
}


Int128 ColumnDateTime64::At(size_t n) const {
    return data_->At(n);
}

void ColumnDateTime64::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDateTime64>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDateTime64::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnDateTime64::Save(CodedOutputStream* output) {
    data_->Save(output);
}

void ColumnDateTime64::Clear() {
    data_->Clear();
}
size_t ColumnDateTime64::Size() const {
    return data_->Size();
}

ItemView ColumnDateTime64::GetItem(size_t index) const {
    return data_->GetItem(index);
}

void ColumnDateTime64::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnDateTime64&>(other);
    data_.swap(col.data_);
}

ColumnRef ColumnDateTime64::Slice(size_t begin, size_t len) {
    auto col = data_->Slice(begin, len)->As<ColumnDecimal>();
    size_t precision = col->Type()->As<DateTime64Type>()->GetPrecision();
    auto result = std::make_shared<ColumnDateTime64>(precision); // TODO FIXME

    result->data_->Append(col);

    return result;
}

}
