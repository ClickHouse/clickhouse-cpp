#pragma once

#include "base/input.h"
#include "base/coded.h"
#include "types.h"

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

    template <typename T>
    inline T* As() {
        return dynamic_cast<T*>(this);
    }

    template <typename T>
    inline const T* As() const {
        return dynamic_cast<const T*>(this);
    }

    /// Get type object of the column.
    virtual TypeRef Type() const = 0;

    /// Count of rows in the column.
    virtual size_t Size() const = 0;

    /// Write value at the given row to the output.
    virtual bool Print(std::basic_ostream<char>& output, size_t row) = 0;

    /// Loads column data from input stream.
    virtual bool Load(CodedInputStream* input, size_t rows) = 0;

    /// Save column data to output stream.
    virtual void Save(CodedOutputStream* output) = 0;
};

using ColumnRef = std::shared_ptr<Column>;


class ColumnFixedString : public Column {
public:
    explicit ColumnFixedString(size_t n);

    /// Append one element to the column.
    void Append(const std::string& str);

    const std::string& operator [] (size_t n) const {
        return data_[n];
    }

    TypeRef Type() const override;

    size_t Size() const override;

    bool Print(std::basic_ostream<char>& output, size_t row) override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    const size_t string_size_;
    std::vector<std::string> data_;
    TypeRef type_;
};

class ColumnString : public Column {
public:
    /// Append one element to the column.
    void Append(const std::string& str);

    const std::string& operator [] (size_t n) const {
        return data_[n];
    }

    TypeRef Type() const override;

    size_t Size() const override;

    bool Print(std::basic_ostream<char>& output, size_t row) override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    std::vector<std::string> data_;
};


class ColumnTuple : public Column {
public:
    ColumnTuple(const std::vector<ColumnRef>& columns);

    ColumnRef operator [] (size_t n) const {
        return columns_[n];
    }

    TypeRef Type() const override;

    size_t Size() const override;

    bool Print(std::basic_ostream<char>& output, size_t row) override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    std::vector<ColumnRef> columns_;
    TypeRef type_;
};


template <typename T>
class ColumnVector : public Column {
public:
    /// Append one element to the column.
    void Append(const T& value) {
        data_.push_back(value);
    }

    const T& operator [] (size_t n) const {
        return data_[n];
    }

    TypeRef Type() const override {
        return type_;
    }

    size_t Size() const override {
        return data_.size();
    }

    bool Print(std::basic_ostream<char>& output, size_t row) override {
        output << data_.at(row);
        return true;
    }

    bool Load(CodedInputStream* input, size_t rows) override {
        data_.resize(rows);

        return input->ReadRaw(data_.data(), data_.size() * sizeof(T));
    }

    void Save(CodedOutputStream* output) override {
        output->WriteRaw(data_.data(), data_.size() * sizeof(T));
    }

protected:
    std::vector<T> data_;
    TypeRef type_ = Type::CreateSimple<T>();
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


class ColumnDate : public ColumnVector<uint16_t> {
public:
    ColumnDate() {
        type_ = Type::CreateDate();
    }
};


class ColumnDateTime : public ColumnVector<uint32_t> {
public:
    ColumnDateTime() {
        type_ = Type::CreateDateTime();
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
