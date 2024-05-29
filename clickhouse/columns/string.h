#pragma once

#include "column.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <deque>

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
    inline std::string_view operator [] (size_t n) const { return At(n); }

    /// Returns the max size of the fixed string
    size_t FixedSize() const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t) override;
    size_t Capacity() const override;

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
    size_t MemoryUsage() const override;

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

    // Estimation on average size of the value in column,
    // helps to reduce used memory and number of re-allocation.
    // Choosing a bad estimation woudn't crash the program,
    // but may cause more frequent smaller memory allocations,
    // reducing overall performance.
    // int32_t to be able to validate againts (unintentional) negative values in ColumnString c-tor.
    // Otherwise those just silently underflow unsigned type,
    // resulting in attempt to allocate enormous amount of memory at run time.
    enum class EstimatedValueSize : int32_t {
        TINY = 8,
        SMALL = 32,
        MEDIUM = 128,
        LARGE = 512,
    };

    // Memory for item storage is not pre-allocated on Reserve(), same as old behaviour.
    static constexpr auto NO_PREALLOCATE = EstimatedValueSize(0);

    explicit ColumnString(EstimatedValueSize value_size_estimation = NO_PREALLOCATE);
    explicit ColumnString(size_t element_count, EstimatedValueSize value_size_estimation = NO_PREALLOCATE);
    explicit ColumnString(const std::vector<std::string> & data);
    explicit ColumnString(std::vector<std::string>&& data);

    ~ColumnString();

    ColumnString& operator=(const ColumnString&) = delete;
    ColumnString(const ColumnString&) = delete;

    /// Change how memory is allocated for future Reserve() or Append() calls. Doesn't affect items that are already added to the column.
    void SetEstimatedValueSize(EstimatedValueSize value_size_estimation);

    /// Appends one element to the column.
    void Append(std::string_view str);

    /// Appends one element to the column.
    void Append(const char* str);

    /// Appends one element to the column.
    void Append(std::string&& steal_value);

    /// Appends one element to the column.
    /// If str lifetime is managed elsewhere and guaranteed to outlive the Block sent to the server
    void AppendNoManagedLifetime(std::string_view str);

    /// Returns element at given row number.
    std::string_view At(size_t n) const;

    /// Returns element at given row number.
    inline std::string_view operator [] (size_t n) const { return At(n); }

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Returns the capacity of the column
    size_t Capacity() const override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    size_t MemoryUsage() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t) const override;

private:
    struct Block;

    void AppendUnsafe(std::string_view);
    Block & PrepareBlockWithSpaceForAtLeast(size_t minimum_required_bytes);

private:

    std::vector<std::string_view> items_;
    std::vector<Block> blocks_;
    std::deque<std::string> append_data_;

    uint32_t value_size_estimation_ = 0;
    size_t next_block_size_ = 0;
};

}
