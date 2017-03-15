#include "date.h"

namespace clickhouse {

ColumnDate::ColumnDate()
    : Column(Type::CreateDate())
    , data_(std::make_shared<ColumnUInt16>())
{
}

void ColumnDate::Append(const std::time_t& value) {
    data_->Append(static_cast<uint16_t>(value / 86400));
}

std::time_t ColumnDate::At(size_t n) const {
    return data_->At(n) * 86400;
}

void ColumnDate::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDate>()) {
        for (size_t i = 0; i < col->data_->Size(); ++i) {
            data_->Append(col->data_->At(i));
        }
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

    for (size_t i = 0; i < col->Size(); ++i) {
        result->data_->Append(col->At(i));
    }

    return result;
}

}
