#include "string.h"
#include "utils.h"

#include "../base/wire_format.h"

namespace clickhouse {

ColumnFixedString::ColumnFixedString(size_t n)
    : Column(Type::CreateString(n))
    , string_size_(n)
{
}

void ColumnFixedString::Append(const std::string& str) {
    data_.push_back(str);
    data_.back().resize(string_size_);
}

void ColumnFixedString::Clear() {
    data_.clear();
}

const std::string& ColumnFixedString::At(size_t n) const {
    return data_.at(n);
}

const std::string& ColumnFixedString::operator [] (size_t n) const {
    return data_[n];
}

size_t ColumnFixedString::FixedSize() const
{
       return string_size_;
}

void ColumnFixedString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnFixedString>()) {
        if (string_size_ == col->string_size_) {
            data_.insert(data_.end(), col->data_.begin(), col->data_.end());
        }
    }
}

bool ColumnFixedString::Load(CodedInputStream* input, size_t rows) {
    for (size_t i = 0; i < rows; ++i) {
        std::string s;
        s.resize(string_size_);

        if (!WireFormat::ReadBytes(input, &s[0], s.size())) {
            return false;
        }

        data_.push_back(s);
    }

    return true;
}

void ColumnFixedString::Save(CodedOutputStream* output) {
    for (size_t i = 0; i < data_.size(); ++i) {
        WireFormat::WriteBytes(output, data_[i].data(), string_size_);
    }
}

size_t ColumnFixedString::Size() const {
    return data_.size();
}

ColumnRef ColumnFixedString::Slice(size_t begin, size_t len) {
    auto result = std::make_shared<ColumnFixedString>(string_size_);

    if (begin < data_.size()) {
        result->data_ = SliceVector(data_, begin, len);
    }

    return result;
}


ColumnString::ColumnString()
    : Column(Type::CreateString())
{
}

ColumnString::ColumnString(const std::vector<std::string>& data)
    : Column(Type::CreateString())
    , data_(data)
{
}

void ColumnString::Append(const std::string& str) {
    data_.push_back(str);
}

void ColumnString::Clear() {
    data_.clear();
}

const std::string& ColumnString::At(size_t n) const {
    return data_.at(n);
}

const std::string& ColumnString::operator [] (size_t n) const {
    return data_[n];
}

void ColumnString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnString>()) {
        data_.insert(data_.end(), col->data_.begin(), col->data_.end());
    }
}

bool ColumnString::Load(CodedInputStream* input, size_t rows) {
    for (size_t i = 0; i < rows; ++i) {
        std::string s;

        if (!WireFormat::ReadString(input, &s)) {
            return false;
        }

        data_.push_back(s);
    }

    return true;
}

void ColumnString::Save(CodedOutputStream* output) {
    for (auto si = data_.begin(); si != data_.end(); ++si) {
        WireFormat::WriteString(output, *si);
    }
}

size_t ColumnString::Size() const {
    return data_.size();
}

ColumnRef ColumnString::Slice(size_t begin, size_t len) {
    return std::make_shared<ColumnString>(SliceVector(data_, begin, len));
}

}
