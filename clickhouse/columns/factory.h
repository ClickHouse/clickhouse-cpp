#pragma once

#include "column.h"

namespace clickhouse {

struct CreateColumnByTypeSettings
{
};

ColumnRef CreateColumnByType(const std::string& type_name, CreateColumnByTypeSettings settings = {});

}
