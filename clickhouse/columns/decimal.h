#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

/**
 * Represents a column of decimal type.
 */
class ColumnDecimal : public Column {
public:
    using ValueType = Int128;

    ColumnDecimal(size_t precision, size_t scale);

    void Append(const Int128& value);
    void Append(const std::string& value);

    Int128 At(size_t i) const;

public:
    void Append(ColumnRef column) override;
    void Clear() override;
    size_t Size() const override;
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t index) const override;
    void SetSerializationKind(Serialization::Kind kind)  override;

    size_t GetScale() const;
    size_t GetPrecision() const;

private:
    bool LoadBody(InputStream* input, size_t rows);
    void SaveBody(OutputStream* output);
    friend SerializationDefault<ColumnDecimal>;

    /// Depending on a precision it can be one of:
    ///  - ColumnInt32
    ///  - ColumnInt64
    ///  - ColumnInt128
    ColumnRef data_;

    explicit ColumnDecimal(TypeRef type, ColumnRef data);
};

}
