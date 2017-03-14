#pragma once

#include "clickhouse/columns/column.h"

namespace clickhouse {

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

}
