#pragma once

#include "varint.h"
#include "io/input.h"

#include <cstdint>
#include <string>
#include <vector>

namespace clickhouse {

class Column {
public:
    virtual ~Column()
    { }

    virtual bool Load(io::CodedInputStream* input, size_t rows) = 0;
};

class ColumnString : public Column {
public:
    bool Load(io::CodedInputStream* input, size_t rows) override {
        for (size_t i = 0; i < rows; ++i) {
            std::string s;

            if (!WireFormat::ReadString(input, &s)) {
                return false;
            }

            data_.push_back(s);
        }
    }

    size_t Size() const {
        return data_.size();
    }

    const std::string& operator [] (size_t n) const {
        return data_[n];
    }

private:
    std::vector<std::string> data_;
};

template <typename T>
class ColumnVector : public Column {
public:
    bool Load(io::CodedInputStream* input, size_t rows) override {
        data_.resize(rows);

        return input->ReadRaw(data_.data(), data_.size() * sizeof(T));
    }

    size_t Size() const {
        return data_.size();
    }

    const T& operator [] (size_t n) const {
        return data_[n];
    }

private:
    std::vector<T> data_;
};


using ColumnUInt64 = ColumnVector<uint64_t>;

}
