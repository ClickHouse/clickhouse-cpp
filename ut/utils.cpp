#include "utils.h"

#include <clickhouse/block.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

namespace {
using namespace clickhouse;
struct DateTimeValue {
    explicit DateTimeValue(const time_t & v)
        : value(v)
    {}

    template <typename T>
    explicit DateTimeValue(const T & v)
        : value(v)
    {}

    const time_t value;
};

std::ostream& operator<<(std::ostream & ostr, const DateTimeValue & time) {
    const auto t = std::localtime(&time.value);
    char buffer[] = "2015-05-18 07:40:12\0\0";
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", t);

    return ostr << buffer;
}

template <typename ColumnType, typename AsType = typename ColumnType::ValueType>
bool doPrintValue(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    if (const auto & casted_c = c->As<ColumnType>()) {
        ostr << static_cast<AsType>(casted_c->At(row));
        return true;
    }
    return false;
}

std::ostream & printColumnValue(const ColumnRef& c, const size_t row, std::ostream & ostr) {
    const auto r = false
        || doPrintValue<ColumnString>(c, row, ostr)
        || doPrintValue<ColumnFixedString>(c, row, ostr)
        || doPrintValue<ColumnUInt8, unsigned int>(c, row, ostr)
        || doPrintValue<ColumnUInt32>(c, row, ostr)
        || doPrintValue<ColumnUInt16>(c, row, ostr)
        || doPrintValue<ColumnUInt64>(c, row, ostr)
        || doPrintValue<ColumnInt8, int>(c, row, ostr)
        || doPrintValue<ColumnInt32>(c, row, ostr)
        || doPrintValue<ColumnInt16>(c, row, ostr)
        || doPrintValue<ColumnInt64>(c, row, ostr)
        || doPrintValue<ColumnFloat32>(c, row, ostr)
        || doPrintValue<ColumnFloat64>(c, row, ostr)
        || doPrintValue<ColumnEnum8>(c, row, ostr)
        || doPrintValue<ColumnEnum16>(c, row, ostr)
        || doPrintValue<ColumnDate, DateTimeValue>(c, row, ostr)
        || doPrintValue<ColumnDateTime, DateTimeValue>(c, row, ostr)
        || doPrintValue<ColumnDateTime64, DateTimeValue>(c, row, ostr)
        || doPrintValue<ColumnDecimal>(c, row, ostr)
    /*    || doPrintValue<ColumnIPv4>(c, row, ostr)
        || doPrintValue<ColumnIPv6>(c, row, ostr)*/;
    if (!r)
        ostr << "Unable to print value of type " << c->GetType().GetName();

    return ostr;
}

}

std::ostream& operator<<(std::ostream & ostr, const Block & block) {
    if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
        return ostr;

    for (size_t col = 0; col < block.GetColumnCount(); ++col) {
        const auto & c = block[col];
        ostr << c->GetType().GetName() << " [";

        for (size_t row = 0; row < block.GetRowCount(); ++row) {
            printColumnValue(c, row, ostr);
            if (row != block.GetRowCount() - 1)
                ostr << ", ";
        }
        ostr << "]";

        if (col != block.GetColumnCount() - 1)
            ostr << "\n";
    }

    return ostr;
}
