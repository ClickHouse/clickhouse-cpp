#pragma once

#include "column.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <algorithm>
#include <type_traits>
#include <deque>

namespace clickhouse {

constexpr size_t DEFAULT_BLOCK_SIZE = 4096;

template <typename>
inline constexpr bool always_false_v = false;

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
    explicit ColumnString(std::vector<std::string>&& data);
    ColumnString& operator=(const ColumnString&) = delete;
    ColumnString(const ColumnString&) = delete;

    /// Returns element at given row number.
    std::string_view At(size_t n) const;

    /// Returns element at given row number.
    std::string_view operator [] (size_t n) const;

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
    void AppendUnsafe(std::string_view);

private:
    struct Block {
        using CharT = typename std::string::value_type;

        explicit Block(size_t starting_capacity)
            : size(0),
            capacity(starting_capacity),
            data_(new CharT[capacity])
        {}

        inline auto GetAvailable() const
        {
            return capacity - size;
        }

        std::string_view AppendUnsafe(std::string_view str)
        {
            const auto pos = &data_[size];

            memcpy(pos, str.data(), str.size());
            size += str.size();

            return std::string_view(pos, str.size());
        }

        auto GetCurrentWritePos()
        {
            return &data_[size];
        }

        std::string_view ConsumeTailAsStringViewUnsafe(size_t len)
        {
            const auto start = &data_[size];
            size += len;
            return std::string_view(start, len);
        }

        size_t size;
        const size_t capacity;
        std::unique_ptr<CharT[]> data_;
    };

    std::vector<std::string_view> items_;
    std::vector<Block> blocks_;
    std::deque<std::string> append_data_;

public:
    /// Appends one element to the column. Copy or move str
    template<typename StringType>
    void Append(StringType&& str) {
        using str_type = decltype(str);
        if (std::is_same_v<std::string, std::decay_t<str_type>> && std::is_rvalue_reference_v<str_type>) {
            append_data_.emplace_back(std::move(str));
            auto& last_data = append_data_.back();
            items_.emplace_back(std::string_view{ last_data.data(),last_data.length() });
        }
        else if constexpr (std::is_convertible_v<std::decay_t<str_type>, std::string_view>) {
            auto data_view = std::string_view(str);
            if (blocks_.size() == 0 || blocks_.back().GetAvailable() < data_view.length()) {
                blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, data_view.size()));
            }
            items_.emplace_back(blocks_.back().AppendUnsafe(data_view));
        }
        else {
            static_assert(always_false_v<str_type>, "the StringType is not correct");
        }
    }

    /// Appends one element to the column.
    /// If str lifetime is managed elsewhere and guaranteed to outlive the Block sent to the server
    void AppendNoManagedLifetime(std::string_view str) {
        items_.emplace_back(str);
    }
};

}
