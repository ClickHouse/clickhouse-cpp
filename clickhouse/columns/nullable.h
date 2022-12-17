#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

/**
 * Represents column of Nullable(T).
 */
class ColumnNullable : public Column {
public:
    ColumnNullable(ColumnRef nested, ColumnRef nulls);

    /// Appends one null flag to the end of the column
    void Append(bool isnull);

    /// Returns null flag at given row number.
    bool IsNull(size_t n) const;

    /// Returns nested column.
    ColumnRef Nested() const;

    /// Returns nulls column.
    ColumnRef Nulls() const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column&) override;

    ItemView GetItem(size_t) const override;

    void SetSerializationKind(Serialization::Kind kind)  override;

private:
    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows);

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows);

    /// Saves column prefix to output stream.
    void SavePrefix(OutputStream* output);

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output);

    friend SerializationDefault<ColumnNullable>;

    ColumnRef nested_;
    std::shared_ptr<ColumnUInt8> nulls_;
};

}
