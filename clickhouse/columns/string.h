#pragma once

#include "clickhouse/columns/column.h"

namespace clickhouse {

/** */
class ColumnFixedString : public Column {
public:
    explicit ColumnFixedString(size_t n);

    /// Appends one element to the column.
    void Append(const std::string& str);

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    const std::string& operator [] (size_t n) const;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }

private:
    const size_t string_size_;
    std::vector<std::string> data_;
    TypeRef type_;
};

/** */
class ColumnString : public Column {
public:
    ColumnString();

    /// Appends one element to the column.
    void Append(const std::string& str);

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    const std::string& operator [] (size_t n) const;

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }

private:
    std::vector<std::string> data_;
};

}
