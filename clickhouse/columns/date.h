#pragma once

#include "clickhouse/columns/numeric.h"

namespace clickhouse {

/** */
class ColumnDate : public ColumnVector<uint16_t> {
public:
    ColumnDate() {
        type_ = Type::CreateDate();
    }
};

/** */
class ColumnDateTime : public ColumnVector<uint32_t> {
public:
    ColumnDateTime() {
        type_ = Type::CreateDateTime();
    }
};

}
