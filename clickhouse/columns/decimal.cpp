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

ColumnDecimal::ColumnDecimal(TypeRef type, ColumnRef data)
    : Column(type),
      data_(data)
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
    auto end = value.end();
    bool sign = true;
    bool has_dot = false;

    int zeros = 0;

    while (c != end) {
        if (*c == '-') {
            sign = false;
            if (c != value.begin()) {
                break;
            }
        } else if (*c == '.' && !has_dot) {
            size_t distance = std::distance(c, end) - 1;
            auto scale = type_->As<DecimalType>()->GetScale();

            if (distance <= scale) {
                zeros = scale - distance;
            } else {
                std::advance(end, scale - distance);
            }

            has_dot = true;
        } else if (*c >= '0' && *c <= '9') {
            if (__builtin_mul_overflow(int_value, 10, &int_value) ||
                __builtin_add_overflow(int_value, *c - '0', &int_value)) {
                throw std::runtime_error("value is too big for 128-bit integer");
            }
        } else {
            throw std::runtime_error(std::string("unexpected symbol '") + (*c) + "' in decimal value");
        }
        ++c;
    }

    if (c != end) {
        throw std::runtime_error("unexpected symbol '-' in decimal value");
    }

    while (zeros) {
        if (__builtin_mul_overflow(int_value, 10, &int_value)) {
            throw std::runtime_error("value is too big for 128-bit integer");
        }
        --zeros;
    }

    Append(sign ? int_value : -int_value);
}

Int128 ColumnDecimal::At(size_t i) const {
    switch (data_->Type()->GetCode()) {
        case Type::Int32:
            return static_cast<Int128>(data_->As<ColumnInt32>()->At(i));
        case Type::Int64:
            return static_cast<Int128>(data_->As<ColumnInt64>()->At(i));
        case Type::Int128:
            return data_->As<ColumnInt128>()->At(i);
        default:
            throw std::runtime_error("Invalid data_ column type in ColumnDecimal");
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
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->Slice(begin, len))};
}

void ColumnDecimal::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDecimal &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDecimal::GetItem(size_t index) const {
    return data_->GetItem(index);
}

size_t ColumnDecimal::GetScale() const
{
    return type_->As<DecimalType>()->GetScale();
}

size_t ColumnDecimal::GetPrecision() const
{
    return type_->As<DecimalType>()->GetPrecision();
}

}
