#include "../columns/itemview.h"

namespace clickhouse {

void ItemView::ValidateData(Type::Code type, DataType data) {
    static const int ANY = -1; // value can be of any size
    static const int ERR = -2; // value is not allowed inside ItemView
    static const int value_size_by_type[] = {
        0,   /*Void*/
        1,   /*Int8*/
        2,   /*Int16*/
        4,   /*Int32*/
        8,   /*Int64*/
        1,   /*UInt8*/
        2,   /*UInt16*/
        4,   /*UInt32*/
        8,   /*UInt64*/
        4,   /*Float32*/
        8,   /*Float64*/
        ANY, /*String*/
        ANY, /*FixedString*/
        4,   /*DateTime*/
        8,   /*DateTime64*/
        2,   /*Date*/
        ERR, /*Array*/
        ERR, /*Nullable*/
        ERR, /*Tuple*/
        1,   /*Enum8*/
        2,   /*Enum16*/
        16,  /*UUID*/
        4,   /*IPv4*/
        8,   /*IPv6*/
        16,  /*Int128*/
        16,  /*Decimal*/
        4,   /*Decimal32*/
        8,   /*Decimal64*/
        16,  /*Decimal128*/
        ERR, /*LowCardinality*/
    };

    if (type >= sizeof(value_size_by_type)/sizeof(value_size_by_type[0]) || type < 0) {
        throw std::runtime_error("Unknon type code:" + std::to_string(static_cast<int>(type)));
    } else {
        const auto expected_size = value_size_by_type[type];
        if (expected_size == ERR) {
            throw std::runtime_error("Unsupported type in ItemView: " + std::to_string(static_cast<int>(type)));
        } else if (expected_size != ANY && expected_size != static_cast<int>(data.size())) {
            throw std::runtime_error("Value size mismatch for type "
                    + std::to_string(static_cast<int>(type)) + " expected: "
                    + std::to_string(expected_size) + ", got: " + std::to_string(data.size()));
        }
    }
}

}
