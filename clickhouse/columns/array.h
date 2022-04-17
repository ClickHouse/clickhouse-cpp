#pragma once

#include "column.h"
#include "numeric.h"

#include <memory>

namespace clickhouse {

/**
 * Represents column of Array(T).
 */
class ColumnArray : public Column {
public:
    ColumnArray(ColumnRef data);
    ColumnArray(ColumnArray && array);

    /// Converts input column to array and appends
    /// as one row to the current column.
    void AppendAsColumn(ColumnRef array);

    /// Convets array at pos n to column.
    /// Type of element of result column same as type of array element.
    ColumnRef GetAsColumn(size_t n) const;

//    template <typename NestedColumnType>
//     GetAsColumnOf(size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream.
    void SavePrefix(OutputStream* output) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t, size_t) const override;

    void Swap(Column&) override;

    void OffsetsIncrease(size_t);

protected:
    size_t GetOffset(size_t n) const;
    size_t GetSize(size_t n) const;
    ColumnRef GetData();
    void AddOffset(size_t n);
    void Reset();

private:
    ColumnRef data_;
    std::shared_ptr<ColumnUInt64> offsets_;
};

template <typename NestedColumnType>
class ColumnArrayT : public ColumnArray {
public:
    class ArrayWrapper;
    using ValueType = ArrayWrapper;

    class ArrayWrapper {
        const std::shared_ptr<NestedColumnType> typed_nested_data_;
        const size_t offset_;
        const size_t size_;

    public:
        using ValueType = typename NestedColumnType::ValueType;

        ArrayWrapper(std::shared_ptr<NestedColumnType> data, size_t offset = 0, size_t size = std::numeric_limits<size_t>::max())
            : typed_nested_data_(data)
            , offset_(offset)
            , size_(std::min(typed_nested_data_->Size() - offset, size))
        {}

        inline const ValueType & operator[](size_t index) const {
            return (*typed_nested_data_)[offset_ + index];
        }

        inline const ValueType & At(size_t index) const {
            return typed_nested_data_->At(offset_ + index);
        }

        class Iterator {
            const std::shared_ptr<NestedColumnType> typed_nested_data_;
            const size_t offset_;
            const size_t size_;
            size_t index_;
        public:
            Iterator(std::shared_ptr<NestedColumnType> typed_nested_data, size_t offset, size_t size, size_t index)
                : typed_nested_data_(typed_nested_data)
                , offset_(offset)
                , size_(size)
                , index_(index)
            {}

            using ValueType = typename NestedColumnType::ValueType;

            inline const ValueType& operator*() const {
                return typed_nested_data_->At(offset_ + index_);
            }

            const ValueType* operator->() const {
                return &typed_nested_data_->At(offset_ + index_);
            }

            inline Iterator& operator++() {
                ++index_;
                return *this;
            }

            inline bool operator==(const Iterator& other) const {
                return this->typed_nested_data_ == other.typed_nested_data_
                        && this->offset_ == other.offset_
                        && this->size_ == other.size_
                        && this->index_ == other.index_;
            }

            inline bool operator!=(const Iterator& other) const {
                return !(*this == other);
            }
        };

        // stl-like interface
        inline Iterator begin() const {
            return Iterator{typed_nested_data_, offset_, size_, 0};
        }

        inline Iterator cbegin() const {
            return Iterator{typed_nested_data_, offset_, size_, 0};
        }

        inline Iterator end() const {
            return Iterator{typed_nested_data_, offset_, size_, size_};
        }

        inline Iterator cend() const {
            return Iterator{typed_nested_data_, offset_, size_, size_};
        }

        inline size_t size() const {
            return size_;
        }
    };

    ColumnArrayT(std::shared_ptr<NestedColumnType> data)
        : ColumnArray(data)
        , typed_nested_data_(data)
    {}

    ColumnArrayT(ColumnArray && array)
        : ColumnArray(std::move(array))
        , typed_nested_data_(this->getData()->template AsStrict<NestedColumnType>())
    {}

    template <typename ...Args>
    explicit ColumnArrayT(Args &&... args)
        : ColumnArrayT(std::make_shared<NestedColumnType>(std::forward<Args>(args)...))
    {}

    inline const ArrayWrapper At(size_t index) const {
        return ArrayWrapper{typed_nested_data_, GetOffset(index), GetSize(index)};
    }

    inline const ArrayWrapper operator[](size_t index) const {
        return ArrayWrapper{typed_nested_data_, GetOffset(index), GetSize(index)};
    }

    using ColumnArray::Append;

    template <typename Container>
    inline void Append(const Container& container) {
        Append(std::begin(container), std::end(container));
    }

    template <typename Begin, typename End>
    inline void Append(Begin begin, const End & end) {
        auto & nested_data = *typed_nested_data_;
        size_t counter = 0;

        while (begin != end) {
            nested_data.Append(*begin);
            ++begin;
            ++counter;
        }

        // Even if there are 0 items, increase counter, creating empty array item.
        AddOffset(counter);
    }

private:
    std::shared_ptr<NestedColumnType> typed_nested_data_;
};

}
