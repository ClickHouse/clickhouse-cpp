#pragma once

#include "io/input.h"
#include "varint.h"

#include <cstdint>
#include <memory>
#include <ostream>
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

    /// Write value at the given row to the output.
    virtual bool Print(std::basic_ostream<char>& output, size_t row) = 0;

    /// Loads column data from input stream.
    virtual bool Load(io::CodedInputStream* input, size_t rows) = 0;
};

using ColumnRef = std::shared_ptr<Column>;


class ColumnFixedString : public Column {
public:
    explicit ColumnFixedString(size_t n)
        : string_size_(n)
    {
    }

    bool Print(std::basic_ostream<char>& output, size_t row) override {
        output << data_.at(row);
        return true;
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
    bool Print(std::basic_ostream<char>& output, size_t row) override {
        output << data_.at(row);
        return true;
    }

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


class ColumnTuple : public Column {
public:
    ColumnTuple(const std::vector<ColumnRef>& columns)
        : columns_(columns)
    {
    }

    bool Print(std::basic_ostream<char>& output, size_t row) override {
        for (auto ci = columns_.begin(); ci != columns_.end(); ) {
            if (!(*ci)->Print(output, row)) {
                return false;
            }

            if (++ci != columns_.end()) {
                output << ", ";
            }
        }

        return true;
    }

    bool Load(io::CodedInputStream* input, size_t rows) override {
        for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
            if (!(*ci)->Load(input, rows)) {
                return false;
            }
        }

        return true;
    }

private:
    std::vector<ColumnRef> columns_;
};

template <typename T>
class ColumnVector : public Column {
public:
    bool Print(std::basic_ostream<char>& output, size_t row) override {
        output << data_.at(row);
        return true;
    }

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

protected:
    std::vector<T> data_;
};

class ColumnUInt8 : public ColumnVector<uint8_t> {
public:
    bool Print(std::basic_ostream<char>& output, size_t row) override {
        output << (unsigned int)data_.at(row);
        return true;
    }
};

class ColumnInt8 : public ColumnVector<int8_t> {
public:
    bool Print(std::basic_ostream<char>& output, size_t row) override {
        output << (int)data_.at(row);
        return true;
    }
};


using ColumnUInt16  = ColumnVector<uint16_t>;
using ColumnUInt32  = ColumnVector<uint32_t>;
using ColumnUInt64  = ColumnVector<uint64_t>;

using ColumnInt16   = ColumnVector<int16_t>;
using ColumnInt32   = ColumnVector<int32_t>;
using ColumnInt64   = ColumnVector<int64_t>;

using ColumnFloat32 = ColumnVector<float>;
using ColumnFloat64 = ColumnVector<double>;

ColumnRef CreateColumnByType(const std::string& type_name);

}
