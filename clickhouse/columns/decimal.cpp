#include "decimal.h"

namespace clickhouse {

ColumnDecimal::ColumnDecimal(size_t precision, size_t scale)
    : Column(Type::CreateDecimal(precision, scale))
{
    if (precision <= 9) {
        data_ = std::make_shared<ColumnInt32>();
    } else if (precision <= 18) {
        data_ = std::make_shared<ColumnInt64>();
    } else {
        data_ = std::make_shared<ColumnInt128>();
    }
}

ColumnDecimal::ColumnDecimal(TypeRef type)
    : Column(type)
{
}

void ColumnDecimal::Append(const Int128& value) {
    if (data_->Type()->GetCode() == Type::Int32) {
        data_->As<ColumnInt32>()->Append(static_cast<ColumnInt32::DataType>(value));
    } else if (data_->Type()->GetCode() == Type::Int64) {
        data_->As<ColumnInt64>()->Append(static_cast<ColumnInt64::DataType>(value));
    } else {
        data_->As<ColumnInt128>()->Append(static_cast<ColumnInt128::DataType>(value));
    }
}

void ColumnDecimal::Append(const std::string& value) {
    Int128 int_value = 0;
    auto c = value.begin();
    bool sign = true;

    while (c != value.end()) {
        if (*c == '-') {
            sign = false;
            if (c != value.begin()) {
                break;
            }
        } else if (*c == '.') {
            // TODO: compare distance with `scale`
        } else if (*c >= '0' && *c <= '9') {
            int_value = int_value * 10 + (*c - '0');
        } else {
            // TODO: throw exception on unexpected symbol
        }
        ++c;
    }

    if (c != value.end()) {
        // TODO: throw exception about symbols after 'minus'
    }

    Append(sign ? int_value : -int_value);
}

Int128 ColumnDecimal::At(size_t i) const {
    if (data_->Type()->GetCode() == Type::Int32) {
        return static_cast<Int128>(data_->As<ColumnInt32>()->At(i));
    } else if (data_->Type()->GetCode() == Type::Int64) {
        return static_cast<Int128>(data_->As<ColumnInt64>()->At(i));
    } else {
        return data_->As<ColumnInt128>()->At(i);
    }
}

void ColumnDecimal::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDecimal>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDecimal::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnDecimal::Save(CodedOutputStream* output) {
    data_->Save(output);
}

void ColumnDecimal::Clear() {
    data_->Clear();
}

size_t ColumnDecimal::Size() const {
    return data_->Size();
}

ColumnRef ColumnDecimal::Slice(size_t begin, size_t len) {
    std::shared_ptr<ColumnDecimal> slice(new ColumnDecimal(type_));
    slice->data_ = data_->Slice(begin, len);
    return slice;
}

}
