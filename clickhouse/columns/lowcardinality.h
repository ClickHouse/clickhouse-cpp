#pragma once

#include "column.h"
#include "numeric.h"
#include "nullable.h"

#include <functional>
#include <string>
#include <unordered_map>
#include <utility>

namespace clickhouse {

template <typename NestedColumnType>
class ColumnLowCardinalityT;

namespace details {

/** LowCardinalityHashKey used as key in unique items hashmap to abstract away key value
 * (type of which depends on dictionary column) and to reduce likelehood of collisions.
 *
 * In order to dramatically reduce collision rate, we use 2 different hashes from 2 different hash functions.
 * First hash is used in hashtable (to calculate item position).
 * Second one is used as part of key value and accessed via `operator==()` upon collision resolution/detection.
 */
using LowCardinalityHashKey = std::pair<std::uint64_t, std::uint64_t>;

struct LowCardinalityHashKeyHash {
    inline std::size_t operator()(const LowCardinalityHashKey &hash_key) const noexcept {
        return hash_key.first;
    }
};

}

/*
 * LC column contains an "invisible" default item at the beginning of the collection. [default, ...]
 * If the nested type is Nullable, it contains a null-item at the beginning and a default item at the second position. [null, default, ...]
 * Null map is not serialized in LC columns. Instead, nulls are tracked by having an index of 0.
 * */
class ColumnLowCardinality : public Column {
public:
    using UniqueItems = std::unordered_map<details::LowCardinalityHashKey, size_t /*dictionary index*/, details::LowCardinalityHashKeyHash>;

    template <typename T>
    friend class ColumnLowCardinalityT;

private:
    // IMPLEMENTATION NOTE: ColumnLowCardinalityT takes reference to underlying dictionary column object,
    // so make sure to NOT change address of the dictionary object (with reset(), swap()) or with anything else.
    ColumnRef dictionary_column_;
    ColumnRef index_column_;
    UniqueItems unique_items_map_;

public:
    // c-tor makes a deep copy of the dictionary_column.
    explicit ColumnLowCardinality(ColumnRef dictionary_column);
    explicit ColumnLowCardinality(std::shared_ptr<ColumnNullable> dictionary_column);
    ~ColumnLowCardinality();

    /// Appends another LowCardinality column to the end of this one, updating dictionary.
    void Append(ColumnRef /*column*/) override;

    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream.
    void SavePrefix(OutputStream* output) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data.
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of current column, with compacted dictionary
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t index) const override;

    size_t GetDictionarySize() const;
    TypeRef GetNestedType() const;

protected:
    std::uint64_t getDictionaryIndex(std::uint64_t item_index) const;
    void appendIndex(std::uint64_t item_index);
    void removeLastIndex();
    ColumnRef GetDictionary();

    void AppendUnsafe(const ItemView &);

private:
    void Setup(ColumnRef dictionary_column);
    void AppendNullItem();
    void AppendDefaultItem();

public:
    static details::LowCardinalityHashKey computeHashKey(const ItemView &);
};

/** Type-aware wrapper that provides simple convenience interface for accessing/appending individual items.
 */
template <typename DictionaryColumnType>
class ColumnLowCardinalityT : public ColumnLowCardinality {

    DictionaryColumnType& typed_dictionary_;
    const Type::Code type_;

public:
    using WrappedColumnType = DictionaryColumnType;
    // Type this column takes as argument of Append and returns with At() and operator[]
    using ValueType = typename DictionaryColumnType::ValueType;

    template <typename ...Args>
    explicit ColumnLowCardinalityT(Args &&... args)
        : ColumnLowCardinalityT(std::make_shared<DictionaryColumnType>(std::forward<Args>(args)...))
    {}

    // Create LC<T> column from existing T-column, making a deep copy of all contents.
    explicit ColumnLowCardinalityT(std::shared_ptr<DictionaryColumnType> dictionary_col)
        : ColumnLowCardinality(dictionary_col),
          typed_dictionary_(dynamic_cast<DictionaryColumnType &>(*GetDictionary())),
          type_(typed_dictionary_.Type()->GetCode())
    {}

    /// Extended interface to simplify reading/adding individual items.

    /// Returns element at given row number.
    inline ValueType At(size_t n) const {
        return typed_dictionary_.At(getDictionaryIndex(n));
    }

    /// Returns element at given row number.
    inline ValueType operator [] (size_t n) const {
        return typed_dictionary_[getDictionaryIndex(n)];
    }

    // so the non-virtual Append below doesn't shadow Append() from base class when compiled with older compilers.
    using ColumnLowCardinality::Append;

    inline void Append(const ValueType & value) {
        AppendUnsafe(ItemView{type_, value});
    }

    template <typename T>
    inline void AppendMany(const T& container) {
        for (const auto & item : container) {
            Append(item);
        }
    }
};

}
