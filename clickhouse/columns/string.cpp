#include "string.h"

#include "clickhouse/wire_format.h"

namespace clickhouse {

ColumnFixedString::ColumnFixedString(size_t n)
    : string_size_(n)
    , type_(Type::CreateString(n))
{
}

void ColumnFixedString::Append(const std::string& str) {
    data_.push_back(str);
    data_.back().resize(string_size_);
}

TypeRef ColumnFixedString::Type() const {
    return type_;
}

size_t ColumnFixedString::Size() const {
    return data_.size();
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


void ColumnString::Append(const std::string& str) {
    data_.push_back(str);
}

TypeRef ColumnString::Type() const {
    static const TypeRef type(Type::CreateString());
    return type;
}

size_t ColumnString::Size() const {
    return data_.size();
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
    for (size_t i = 0; i < data_.size(); ++i) {
        WireFormat::WriteString(output, data_[i]);
    }
}

}
