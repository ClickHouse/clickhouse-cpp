#pragma once

#include "decimal.h"
#include "numeric.h"

#include <ctime>

namespace clickhouse {

/** */
class ColumnDate : public Column {
public:
    ColumnDate();

    /// Appends one element to the end of column.
    /// TODO: The implementation is fundamentally wrong.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    /// TODO: The implementation is fundamentally wrong.
    std::time_t At(size_t n) const;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Clear column data .
    void Clear() override;
    
    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

private:
    std::shared_ptr<ColumnUInt16> data_;
};

/** */
class ColumnDateTime : public Column {
public:
    ColumnDateTime();

    /// Appends one element to the end of column.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    std::time_t At(size_t n) const;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Clear column data .
    void Clear() override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

private:
    std::shared_ptr<ColumnUInt32> data_;
};


/** */
class ColumnDateTime64 : public Column {
public:
    explicit ColumnDateTime64(size_t);

    /// Appends one element to the end of column.
    void Append(const Int64& value);
    // It is a bit controversal: users might expect it to parse string of ISO8601 or some other human-friendly format,
    // but current implemntation parses it as fractional integer with decimal point, e.g. "123.456".
//    void Append(const std::string& value);

    /// Returns element at given row number.
    Int64 At(size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Clear column data .
    void Clear() override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

    size_t GetPrecision() const;

private:
    ColumnDateTime64(TypeRef type, std::shared_ptr<ColumnDecimal> data);

private:
    std::shared_ptr<ColumnDecimal> data_;
    const size_t precision_;
};

}
