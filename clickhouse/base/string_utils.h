#pragma once

#include <sstream>
#include <string>
#include <string_view>

namespace clickhouse {

template <typename T>
inline T FromString(const std::string_view s) {
   std::istringstream iss((std::string(s)));
   T result;
   iss >> result;
   return result;
}

}
