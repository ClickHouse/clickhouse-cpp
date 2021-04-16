#pragma once

/// In a separate header to allow basic non-Int128 functionality on systems that lack absl.
#include <absl/numeric/int128.h>

namespace clickhouse
{
using Int128 = absl::int128;
}
