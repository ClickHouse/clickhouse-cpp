#pragma once

#include "clickhouse/base/input.h"
#include "clickhouse/base/coded.h"
#include "clickhouse/types/types.h"

namespace clickhouse {

using ColumnRef = std::shared_ptr<class Column>;

/**
 * An abstract base of all columns classes.
 */
class Column {
public:
    explicit inline Column(TypeRef type)
        : type_(type)
    {
    }

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
    inline TypeRef Type() const { return type_; }

    /// Appends content of given column to the end of current one.
    virtual void AppendFromColumn(ColumnRef column) = 0;

    /// Loads column data from input stream.
    virtual bool Load(CodedInputStream* input, size_t rows) = 0;

    /// Save column data to output stream.
    virtual void Save(CodedOutputStream* output) = 0;

    /// Count of rows in the column.
    virtual size_t Size() const = 0;

    /// Makes slice of the current column.
    virtual ColumnRef Slice(size_t begin, size_t len) = 0;

protected:
    TypeRef type_ = nullptr;
};

}
