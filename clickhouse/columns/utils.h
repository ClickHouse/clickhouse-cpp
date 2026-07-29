#pragma once

// Deprecated header. The Wrap helpers (SliceVector, HasWrapMethod, WrapColumn)
// moved into columns/column.h. Include "clickhouse/columns/column.h" instead.
// This forwarding stub is kept for backward compatibility and will be removed
// in a future release.
#if defined(__GNUC__) || defined(__clang__) || (defined(_MSC_VER) && _MSC_VER >= 1929)
#  warning "clickhouse/columns/utils.h is deprecated; include \"clickhouse/columns/column.h\" instead"
#else
#  pragma message("clickhouse/columns/utils.h is deprecated; include \"clickhouse/columns/column.h\" instead")
#endif

#include "column.h"
