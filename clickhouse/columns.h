#pragma once

#include "base/input.h"
#include "base/coded.h"
#include "types.h"

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

    /// Loads column data from input stream.
    virtual bool Load(CodedInputStream* input, size_t rows) = 0;

    /// Save column data to output stream.
    virtual void Save(CodedOutputStream* output) = 0;
};

using ColumnRef = std::shared_ptr<Column>;

/** */
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

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    const size_t string_size_;
    std::vector<std::string> data_;
    TypeRef type_;
};

/** */
class ColumnString : public Column {
public:
    /// Append one element to the column.
    void Append(const std::string& str);

    const std::string& operator [] (size_t n) const {
        return data_[n];
    }

    TypeRef Type() const override;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    std::vector<std::string> data_;
};

/** */
class ColumnTuple : public Column {
public:
    ColumnTuple(const std::vector<ColumnRef>& columns);

    ColumnRef operator [] (size_t n) const {
        return columns_[n];
    }

    TypeRef Type() const override;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

private:
    std::vector<ColumnRef> columns_;
    TypeRef type_;
};

/** */
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

/** */
class ColumnDate : public ColumnVector<uint16_t> {
public:
    ColumnDate() {
        type_ = Type::CreateDate();
    }
};

/** */
class ColumnDateTime : public ColumnVector<uint32_t> {
public:
    ColumnDateTime() {
        type_ = Type::CreateDateTime();
    }
};

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

template <typename T>
struct TypeToColumn;

/** */
class ColumnArray : public Column {
public:
    ColumnArray(ColumnRef data);

    TypeRef Type() const override;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    template <typename T>
    std::vector<int8_t> GetArray(size_t n) const {
        size_t offset = GetOffset(n);
        size_t size = GetSize(n);
        std::vector<int8_t> result;

        for (size_t i = offset; i < offset + size; ++i) {
            result.push_back((*data_->As<typename TypeToColumn<T>::Column>())[i]);
        }

        return result;
    }

private:
    inline size_t GetOffset(size_t n) const {
        return (n == 0) ? 0 : (*offsets_)[n - 1];
    }

    inline size_t GetSize(size_t n) const {
        return (n == 0) ? (*offsets_)[n] : ((*offsets_)[n] - (*offsets_)[n - 1]);
    }

private:
    ColumnRef data_;
    std::shared_ptr<ColumnUInt64> offsets_;
};

ColumnRef CreateColumnByType(const std::string& type_name);

template <> struct TypeToColumn<int8_t>   { using Column = ColumnInt8; };
template <> struct TypeToColumn<int16_t>  { using Column = ColumnInt16; };
template <> struct TypeToColumn<int32_t>  { using Column = ColumnInt32; };
template <> struct TypeToColumn<int64_t>  { using Column = ColumnInt64; };

template <> struct TypeToColumn<uint8_t>  { using Column = ColumnUInt8; };
template <> struct TypeToColumn<uint16_t> { using Column = ColumnUInt16; };
template <> struct TypeToColumn<uint32_t> { using Column = ColumnUInt32; };
template <> struct TypeToColumn<uint64_t> { using Column = ColumnUInt64; };

}
