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
    virtual TypeRef Type() const = 0;

    /// Count of rows in the column.
    virtual size_t Size() const = 0;

    /// Loads column data from input stream.
    virtual bool Load(CodedInputStream* input, size_t rows) = 0;

    /// Save column data to output stream.
    virtual void Save(CodedOutputStream* output) = 0;
};

}
