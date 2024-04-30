#pragma once

#include "column.h"

namespace clickhouse {

ColumnRef CreateColumnByType(const std::string& type_name);

}
