#pragma once

#include "column.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
const size_t DEFAULT_BLOCK_SIZE = 4096;

template <typename Container>
size_t ComputeTotalSize(const Container& strings, size_t begin = 0, size_t len = -1) {
    size_t result = 0;
    if (begin < strings.size()) {
        len = std::min(len, strings.size() - begin);

        for (size_t i = begin; i < begin + len; ++i) result += strings[i].size();
    }

    return result;
}

}  // namespace

namespace clickhouse {

/**
 * Represents column of fixed-length strings.
 */
class ColumnFixedString : public Column {
public:
    using ValueType = std::string_view;

    explicit ColumnFixedString(size_t n);

    template <typename Values>
    ColumnFixedString(size_t n, const Values & values)
        : ColumnFixedString(n)
    {
        for (const auto & v : values)
            Append(v);
    }

    /// Appends one element to the column.
    void Append(std::string_view str);

    /// Returns element at given row number.
    std::string_view At(size_t n) const;

    /// Returns element at given row number.
    std::string_view operator [] (size_t n) const;

    /// Returns the max size of the fixed string
    size_t FixedSize() const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

    ItemView GetItem(size_t) const override;

private:
    size_t string_size_;
    std::string data_;
};

/**
 * Represents column of variable-length strings.
 */
class ColumnString : public Column {
public:
    // Type this column takes as argument of Append and returns with At() and operator[]
    using ValueType = std::string_view;

    ColumnString();
    ~ColumnString();

    explicit ColumnString(const std::vector<std::string> & data);
    explicit ColumnString(const std::vector<std::string_view>& data);
    ColumnString(std::string&& payload, std::vector<std::string_view>&& items);
    ColumnString& operator=(const ColumnString&) = delete;
    ColumnString(const ColumnString&) = delete;

    /// Appends one element to the column.
    void Append(std::string_view str);

    /// Returns element at given row number.
    std::string_view At(size_t n) const;

    /// Returns element at given row number.
    std::string_view operator [] (size_t n) const;

public:
    /// Reserve column capacity to reduce memory allocation times.
    void Reserve(size_t rows) override;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t) const override;

private:
    template <typename StringVec>
    void ConstructFromVector(const StringVec& data) {
        items_.reserve(data.size());
        blocks_.emplace_back(ComputeTotalSize(data));

        for (const auto& s : data) {
            AppendUnsafe(s);
        }
    }

    void AppendUnsafe(std::string_view);

private:
    struct Block;

    std::vector<std::string_view> items_;
    std::vector<Block> blocks_;
};

}
