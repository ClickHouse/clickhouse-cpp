#pragma once

#include "io/input.h"
#include "varint.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace clickhouse {

/**
 * An abstract base of all columns classes.
 */
class Column {
public:
    virtual ~Column()
    { }

    /// Loads column data from input stream.
    virtual bool Load(io::CodedInputStream* input, size_t rows) = 0;
};


class ColumnFixedString : public Column {
public:
    explicit ColumnFixedString(size_t n)
        : string_size_(n)
    {
    }

    bool Load(io::CodedInputStream* input, size_t rows) override {
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

private:
    const size_t string_size_;
    std::vector<std::string> data_;
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

        return true;
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


using ColumnRef     = std::shared_ptr<Column>;

using ColumnUInt8   = ColumnVector<uint8_t>;
using ColumnUInt16  = ColumnVector<uint16_t>;
using ColumnUInt32  = ColumnVector<uint32_t>;
using ColumnUInt64  = ColumnVector<uint64_t>;

using ColumnInt8    = ColumnVector<int8_t>;
using ColumnInt16   = ColumnVector<int16_t>;
using ColumnInt32   = ColumnVector<int32_t>;
using ColumnInt64   = ColumnVector<int64_t>;

using ColumnFloat32 = ColumnVector<float>;
using ColumnFloat64 = ColumnVector<double>;

ColumnRef CreateColumnByType(const std::string& type_name);

}
