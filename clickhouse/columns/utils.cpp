#include "utils.h"

namespace clickhouse {

std::ostream& writeNumber(std::ostream& o, const __int128& x) {
    return writePODBinary(o, x);
}

std::ostream& writeNumber(std::ostream& o, const int8_t& x) {
    return o << static_cast<int16_t>(x);
}
std::ostream& writeNumber(std::ostream& o, const uint8_t& x) {
    return o << static_cast<uint16_t>(x);
}

}  // namespace clickhouse
