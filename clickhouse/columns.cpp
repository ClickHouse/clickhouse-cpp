#include "columns.h"

#include <sstream>
#include <iostream>

namespace clickhouse {

template <typename T>
inline T from_string(const std::string& s){
   std::istringstream iss;
   iss.str(s);
   T result;
   iss >> result;
   return result;
}

static ColumnRef CreateFixedString(const std::string& name) {
    const char* p = name.data();
    const char* st;

    p += std::string("FixedString").size();
    if (*p != '(') {
        return nullptr;
    } else {
        ++p;
    }

    st = p;
    while (isdigit(*p)) {
        ++p;
    }

    std::string size(st, p);
    if (*p != ')') {
        return nullptr;
    }

    return std::make_shared<ColumnFixedString>(from_string<size_t>(size));
}

ColumnRef CreateColumnByName(const std::string& name) {
    if (name == "UInt8")
        return std::make_shared<ColumnUInt8>();
    if (name == "UInt16")
        return std::make_shared<ColumnUInt16>();
    if (name == "UInt32")
        return std::make_shared<ColumnUInt32>();
    if (name == "UInt64")
        return std::make_shared<ColumnUInt64>();

    if (name == "Int8")
        return std::make_shared<ColumnInt8>();
    if (name == "Int16")
        return std::make_shared<ColumnInt16>();
    if (name == "Int32")
        return std::make_shared<ColumnInt32>();
    if (name == "Int64")
        return std::make_shared<ColumnInt64>();

    if (name == "Float32")
        return std::make_shared<ColumnFloat32>();
    if (name == "Float64")
        return std::make_shared<ColumnFloat64>();

    if (name == "String")
        return std::make_shared<ColumnString>();

    if (name.find("FixedString") == 0)
        return CreateFixedString(name);

    return nullptr;
}

}
