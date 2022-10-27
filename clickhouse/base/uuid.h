#pragma once

#include <cstdint>
#include <utility>

namespace clickhouse {

using UInt128 = std::pair<uint64_t, uint64_t>;

using UUID = UInt128;

}
