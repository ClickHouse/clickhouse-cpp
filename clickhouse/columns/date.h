#pragma once

#include "clickhouse/columns/numeric.h"

namespace clickhouse {

/** */
class ColumnDate : public ColumnVector<uint16_t> {
public:
    ColumnDate() {
        type_ = Type::CreateDate();
    }

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef) override { }

    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }
};

/** */
class ColumnDateTime : public ColumnVector<uint32_t> {
public:
    ColumnDateTime() {
        type_ = Type::CreateDateTime();
    }

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef) override { }

    ColumnRef Slice(size_t, size_t) override { return ColumnRef(); }
};

}
