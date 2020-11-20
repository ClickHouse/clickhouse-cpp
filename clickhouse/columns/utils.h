#pragma once

#include <algorithm>
#include <sstream>  //include this to use string streams
#include <string>
#include <type_traits>
#include <vector>

namespace clickhouse {

template <typename T>
std::vector<T> SliceVector(const std::vector<T>& vec, size_t begin, size_t len) {
    std::vector<T> result;

    if (begin < vec.size()) {
        len = std::min(len, vec.size() - begin);
        result.assign(vec.begin() + begin, vec.begin() + (begin + len));
    }

    return result;
}

/// Methods for outputting the value in text form for a tab-separated format.
template <typename T>
std::enable_if_t<std::is_integral_v<T>, std::ostream&> writeNumber(std::ostream& o, const T& x) {
    return o << x;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, std::ostream&> writeNumber(std::ostream& o, const T& x) {
    return o << x;
}

template <typename T>
std::ostream& writePODBinary(std::ostream& o, const T& t) {
    std::string s(reinterpret_cast<const char*>(t), sizeof(t));
    return o << s;
}

std::ostream& writeNumber(std::ostream& o, const __int128& x);

// https://stackoverflow.com/a/7587829/1203241
std::ostream& writeNumber(std::ostream& o, const int8_t& x);
std::ostream& writeNumber(std::ostream& o, const uint8_t& x);

}  // namespace clickhouse
