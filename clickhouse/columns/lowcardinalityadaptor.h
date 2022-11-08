#pragma once

#include "column.h"
#include "lowcardinality.h"

#include <cassert>

namespace clickhouse {

class OutputStream;
class CodedInputStream;

/** Adapts any ColumnType to be serialized\deserialized as LowCardinality,
 *  and to be castable to ColumnType via ColumnPtr->As<ColumnType>().
 *
 * It helps to ease migration of the old codebases, which can't afford to switch
 * to using ColumnLowCardinalityT or ColumnLowCardinality directly,
 * but still want to benefit from smaller on-wire LowCardinality bandwidth footprint.
 *
 * Not intended to be used by users directly.
 *
 * @see ClientOptions, CreateColumnByType
 */
template <typename AdaptedColumnType>
class LowCardinalitySerializationAdaptor : public AdaptedColumnType
{
public:
    template <class... Args>
    LowCardinalitySerializationAdaptor(Args&&...args)
        : AdaptedColumnType(std::forward<Args>(args)...)
    {
        Column::serialization_ = Serialization::MakeDefault(this);
    }

private:
    friend SerializationDefault<LowCardinalitySerializationAdaptor<AdaptedColumnType>>;

    bool LoadPrefix(InputStream* input, size_t rows) {
        auto new_data_column = this->Slice(0, 0)->template As<AdaptedColumnType>();
        ColumnLowCardinalityT<AdaptedColumnType> low_cardinality_col(new_data_column);

        return low_cardinality_col.GetSerialization()->LoadPrefix(&low_cardinality_col, input, rows);
    }

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) {
        auto new_data_column = this->CloneEmpty()->template As<AdaptedColumnType>();

        ColumnLowCardinalityT<AdaptedColumnType> low_cardinality_col(new_data_column);
        if (!low_cardinality_col.GetSerialization()->LoadBody(&low_cardinality_col, input, rows))
            return false;

        // It safe to reuse `flat_data_column` later since ColumnLowCardinalityT makes a deep copy, but still check just in case.
        assert(new_data_column->Size() == 0);

        for (size_t i = 0; i < low_cardinality_col.Size(); ++i)
            new_data_column->Append(low_cardinality_col[i]);

        this->Swap(*new_data_column);
        return true;
    }

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) {
        ColumnLowCardinalityT<AdaptedColumnType> low_cardinality_col(this->template As<AdaptedColumnType>());
        low_cardinality_col.GetSerialization()->SaveBody(&low_cardinality_col, output);
    }

    void SetSerializationKind(Serialization::Kind kind)  override {
        switch (kind)
        {
        case Serialization::Kind::DEFAULT:
            Column::serialization_ = Serialization::MakeDefault(this);
            break;
        default:
            throw UnimplementedError("Serialization kind:" + std::to_string(static_cast<int>(kind))
                + " is not supported for column of " + Column::Type()->GetName());
        }
    }
};

}
