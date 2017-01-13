#pragma once

#include "string_view.h"

#include <sstream>
#include <string>

namespace clickhouse {

template <typename T>
inline T FromString(const std::string& s) {
   std::istringstream iss;
   iss.str(s);
   T result;
   iss >> result;
   return result;
}

template <typename T>
inline T FromString(const StringView& s) {
   std::istringstream iss;
   iss.str(std::string(s));
   T result;
   iss >> result;
   return result;
}

}
