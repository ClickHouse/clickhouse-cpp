#pragma once

#include <memory>
#include <vector>

#include "../exceptions.h"

namespace clickhouse {

class InputStream;
class OutputStream;

class Column;

using ColumnRef = std::shared_ptr<Column>;

using SerializationRef = std::shared_ptr<class Serialization>;

template <typename C>
class SerializationDefault;

template <typename C, typename T>
class SerializationSparseT;

class Serialization {
public:
    enum class Kind : uint8_t {
        DEFAULT = 0,
        SPARSE  = 1,
    };

    Serialization()          = default;
    virtual ~Serialization() = default;

    virtual Kind GetKind() const = 0;

    /// Loads column prefix from input stream.
    virtual bool LoadPrefix(Column* column, InputStream* input, size_t rows) const = 0;

    /// Loads column data from input stream.
    virtual bool LoadBody(Column* column, InputStream* input, size_t rows) const = 0;

    /// Saves column prefix to output stream. Column types with prefixes must implement it.
    virtual void SavePrefix(Column* column, OutputStream* output) const = 0;

    /// Saves column body to output stream.
    virtual void SaveBody(Column* column, OutputStream* output) const = 0;

    template <typename C>
    inline static std::shared_ptr<SerializationDefault<C>> MakeDefault(C*) {
        return std::make_shared<SerializationDefault<C>>();
    }

    template <typename C, typename T>
    inline static std::shared_ptr<SerializationSparseT<C, T>> MakeSparse(C* column, T default_value) {
        return std::make_shared<SerializationSparseT<C, T>>(MakeDefault(column), std::move(default_value));
    }

protected:
    template <typename C>
    C* ColumnAs(Column* column) const {
        C* result = dynamic_cast<C*>(column);
        if (!result) {
            throw ValidationError("Can't cast column");
        }
        return result;
    }
};

template <typename C>
class SerializationDefault : public Serialization {
public:
    Kind GetKind() const override { return Kind::DEFAULT; }

    /// Loads column prefix from input stream.
    bool LoadPrefix([[maybe_unused]] Column* column, [[maybe_unused]] InputStream* input, [[maybe_unused]] size_t rows) const override {
        if constexpr (HasLoadPrefix<C>::value) {
            return ColumnAs<C>(column)->LoadPrefix(input, rows);
        }
        return true;
    };

    /// Loads column data from input stream.
    bool LoadBody(Column* column, InputStream* input, size_t rows) const override { return ColumnAs<C>(column)->LoadBody(input, rows); };

    /// Saves column prefix to output stream. Column types with prefixes must implement it.
    void SavePrefix([[maybe_unused]] Column* column, [[maybe_unused]] OutputStream* output) const override {
        if constexpr (HasSavePrefix<C>::value) {
            ColumnAs<C>(column)->SavePrefix(output);
        }
    };

    /// Saves column body to output stream.
    void SaveBody(Column* column, OutputStream* output) const override { ColumnAs<C>(column)->SaveBody(output); };

private:
    template <typename T>
    struct HasLoadPrefix {
    private:
        static int detect(...);
        template <typename U>
        static decltype(std::declval<U>().LoadPrefix(std::declval<InputStream*>(), 0)) detect(const U&);

    public:
        static constexpr bool value = std::is_same<bool, decltype(detect(std::declval<T>()))>::value;
    };

    template <typename T>
    struct HasSavePrefix {
    private:
        static int detect(...);
        template <typename U>
        static decltype(std::declval<U>().SavePrefix(std::declval<OutputStream*>())) detect(const U&);

    public:
        static constexpr bool value = std::is_same<void, decltype(detect(std::declval<T>()))>::value;
    };
};

/*
 * The main purpose of sparse serialization is to reduce the amount of transmitted data
 * in the case when the column is filled with default values.
 * Default values depend on the type of column. For instance, it is an empty string in case of string type,
 * zero in case of an integer, etc. Sparse serialization saves and loads only non-default values and their indices.
 * Saving contains the following steps:
 *  - Takes indices all non-default values and writes differences between neighboring to stream;
 *  - Makes a column of the same type containing only non-default values and writes it to stream using nested serialization..
 * Loading makes the opposite work: restores indices and non-default values and appends them to the target column
 */

class SerializationSparse : public Serialization {
public:
    explicit SerializationSparse(SerializationRef nested);

    Kind GetKind() const override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(Column* column, InputStream* input, size_t rows) const override;

    /// Loads column data from input stream.
    bool LoadBody(Column* column, InputStream* input, size_t rows) const override;

    /// Saves column prefix to output stream. Column types with prefixes must implement it.
    void SavePrefix(Column* column, OutputStream* output) const override;

    /// Saves column body to output stream.
    void SaveBody(Column* column, OutputStream* output) const override;

protected:
    virtual std::vector<size_t> GetIndicesOfNonDefaultRows(Column* column) const                                           = 0;
    virtual ColumnRef GetByIndices(Column* column, const std::vector<size_t>& indeces) const                               = 0;
    virtual void AppendSparseColumn(Column* column, size_t rows, const std::vector<size_t>& indices, Column* values) const = 0;

private:
    SerializationRef nested_;
};

template <typename C, typename T>
class SerializationSparseT : public SerializationSparse {
public:
    explicit SerializationSparseT(SerializationRef nested, T default_value)
        : SerializationSparse(std::move(nested)), default_value_(std::move(default_value)) {}

protected:
    std::vector<size_t> GetIndicesOfNonDefaultRows(Column* column) const override {
        C* typed_column = ColumnAs<C>(column);
        std::vector<size_t> result;
        const size_t size = typed_column->Size();
        result.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (typed_column->At(i) != default_value_) {
                result.push_back(i);
            }
        }
        return result;
    }

    ColumnRef GetByIndices(Column* column, const std::vector<size_t>& indeces) const override {
        C* typed_column           = ColumnAs<C>(column);
        std::shared_ptr<C> result = typed_column->CloneEmpty()->template AsStrict<C>();
        for (size_t index : indeces) {
            result->Append(typed_column->At(index));
        }
        return result;
    }

    void AppendSparseColumn(Column* column, size_t rows, const std::vector<size_t>& indices, Column* values) const override {
        C* target = ColumnAs<C>(column);
        C* source = ColumnAs<C>(values);
        auto it   = indices.begin();
        for (size_t i = 0; i < rows; ++i) {
            if (it == indices.end() || i != *it) {
                target->Append(default_value_);
                continue;
            }
            target->Append(source->At(it - indices.begin()));
            ++it;
        }
    }

private:
    T default_value_;
};

}  // namespace clickhouse
