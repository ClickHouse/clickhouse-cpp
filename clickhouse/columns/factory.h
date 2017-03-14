#pragma once

#include "clickhouse/columns/column.h"

namespace clickhouse {

ColumnRef CreateColumnByType(const std::string& type_name);

}
