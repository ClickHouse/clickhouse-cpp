#include "decimal.h"
#include "clickhouse/exceptions.h"

#include <algorithm>
#include <iterator>

namespace {

using clickhouse::ValidationError;

void ValidateDecimalString(const std::string& value, size_t precision, size_t scale)
{
    // Although some of the strings can already be parsed by the existing algorithm without
    // any changes, we do not want to worry about backward-compatibility with weird strings
    // when refactoring and improving the algorithm.
    if (value.empty() || value == "." || value == "-" || value == "-.") {
        throw ValidationError("bad string " + value);
    }

    auto it = value.begin();
    auto end = value.end();
    bool dot_found = false;
    bool padding = true;
    size_t digits = 0;

    if (it < end && *it == '-') {
        ++it;
    }

    for (; it < end; ++it) {
        if (*it == '.' && !dot_found) {
            dot_found = true;
        }
        else if(*it < '0' || *it > '9') {
            throw ValidationError(
                    std::string("unexpected symbol '") + (*it) + "' in decimal value");
        }
        else {
            // start counting digits starting from the first non-0 and until we reached the '.'
            padding &= *it == '0';
            digits += !dot_found & !padding;
        }
    }

    if (dot_found) {
        // when the '.' is not present, then we assume that the last `scale` digits of this
        // string are decimal values. This might not be a good assumption, but we keep it here
        // for backward compatibility with the original version of the library.
        digits += scale;
    }

    if (digits > precision) {
        throw ValidationError(
                std::string("Value ") + value + " is too large for"
                " scale=" + std::to_string(scale) +
                " precision=" + std::to_string(precision));
    }
}

} // anonymous namespace

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
    data_type_code_ = data_->Type()->GetCode();
}

ColumnDecimal::ColumnDecimal(TypeRef type, ColumnRef data)
    : Column(type),
      data_(data),
      data_type_code_(data_->Type()->GetCode())
{
}

void ColumnDecimal::Append(const Int128& value) {
    switch (data_type_code_) {
        case Type::Int32:
            static_cast<ColumnInt32*>(data_.get())->Append(static_cast<int32_t>(Bignum::Int128Low64(value)));
            break;
        case Type::Int64:
            static_cast<ColumnInt64*>(data_.get())->Append(static_cast<int64_t>(Bignum::Int128Low64(value)));
            break;
        default:
            static_cast<ColumnInt128*>(data_.get())->Append(static_cast<Int128>(value));
            break;
    }
}

void ColumnDecimal::Append(const std::string& value) {
    auto scale = type_->As<DecimalType>()->GetScale();
    auto precision = type_->As<DecimalType>()->GetPrecision();

    ValidateDecimalString(value, precision, scale);

    // Normalize the string - produce a string that would be used to be parsed
    // as Int128. In other words, the value must be scaled, such that number of digits after
    // the '.' match the scale. If there less digits - pad them with zeros, if there are more
    // digits - just truncate them. Then the dot must also be removed.
    // WARNING: When the string does not contain any dots, that means it is already normalized,
    // no zeros must be padded.
    // This is highly debatable behavior, but it has been since beginning of the library
    // and we leave it here for backward compatibility.
    std::string cleaned{};
    cleaned.reserve(precision + 1); // extra byte for the '-' sign

    int64_t pad_len = 0;
    auto it = value.cbegin();
    auto end = value.cend();

    if (*it == '-') {
        cleaned.push_back('-');
        ++it;
    }

    for (; it < end && *it != '.'; ++it) {
        cleaned.push_back(*it);
    }

    if (it < end && *it == '.') {
        ++it;
        auto input_scale = std::distance(it, end);
        pad_len = (int64_t)scale - input_scale;
        auto real_end = end;
        if (pad_len <= 0) {
            real_end = it + (int64_t)scale;
        }
        for(; it < real_end; ++it) {
            cleaned.push_back(*it);
        }

        if (pad_len > 0) {
            cleaned.append((size_t)pad_len, '0');
        }
    }

    Append(Bignum::StringToInt128(cleaned));
}

Int128 ColumnDecimal::At(size_t i) const {
    switch (data_type_code_) {
        case Type::Int32:
            return static_cast<Int128>(static_cast<const ColumnInt32*>(data_.get())->At(i));
        case Type::Int64:
            return static_cast<Int128>(static_cast<const ColumnInt64*>(data_.get())->At(i));
        case Type::Int128:
            return static_cast<const ColumnInt128*>(data_.get())->At(i);
        default:
            throw ValidationError("Invalid data_ column type in ColumnDecimal");
    }
}

std::string ColumnDecimal::StringAt(size_t i) const {
    auto scale = GetScale();

    Int128 val = At(i);
    std::string raw_str = Bignum::Int128ToString(val);
    if (scale == 0) {
        return raw_str;
    }

    std::string ret;
    ret.reserve(GetPrecision() + 2); // extra space for '-' and '.';

    auto it = raw_str.cbegin();
    auto end = raw_str.cend();

    if (it != end && *it == '-') {
        ret.push_back(*it++);
    }

    int64_t str_len = std::distance(it, end);
    int64_t integral_len = str_len - static_cast<int64_t>(scale);

    if (integral_len > 0) {
        std::copy(it, it + integral_len, std::back_inserter(ret));
        it += integral_len;
        ret.push_back('.');
    }
    else {
        ret.append("0.");
        ret.append(static_cast<size_t>(-integral_len), '0');
    }
    std::copy(it, end, std::back_inserter(ret));

    return ret;
}

void ColumnDecimal::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

void ColumnDecimal::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDecimal>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDecimal::LoadBody(InputStream * input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDecimal::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

void ColumnDecimal::Clear() {
    data_->Clear();
}

size_t ColumnDecimal::Size() const {
    return data_->Size();
}

ColumnRef ColumnDecimal::Slice(size_t begin, size_t len) const {
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->Slice(begin, len))};
}

ColumnRef ColumnDecimal::CloneEmpty() const {
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->CloneEmpty())};
}

void ColumnDecimal::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDecimal &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDecimal::GetItem(size_t index) const {
    return ItemView{GetType().GetCode(), data_->GetItem(index)};
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
