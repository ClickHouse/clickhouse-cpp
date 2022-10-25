#pragma once

#include "string_view.h"

#include <sstream>
#include <string>

namespace clickhouse {

template <typename T>
[[deprecated("Not used by clickhosue-cpp itself, and will be removed in next major release (3.0) ")]]
inline T FromString(const std::string& s) {
   std::istringstream iss(s);
   T result;
   iss >> result;
   return result;
}

template <typename T>
[[deprecated("Not used by clickhosue-cpp itself, and will be removed in next major release (3.0) ")]]
inline T FromString(const StringView& s) {
   std::istringstream iss((std::string(s)));
   T result;
   iss >> result;
   return result;
}

}
