#include "../columns/itemview.h"

namespace clickhouse {

void ItemView::ValidateData(Type::Code type, DataType data) {
    int expected_size = 0;
    switch (type) {
        case Type::Code::Void:
            expected_size = 0;
            break;

        case Type::Code::Int8:
        case Type::Code::UInt8:
        case Type::Code::Enum8:
            expected_size = 1;
            break;

        case Type::Code::Int16:
        case Type::Code::UInt16:
        case Type::Code::Date:
        case Type::Code::Enum16:
            expected_size = 2;
            break;

        case Type::Code::Int32:
        case Type::Code::UInt32:
        case Type::Code::Float32:
        case Type::Code::DateTime:
        case Type::Code::IPv4:
        case Type::Code::Decimal32:
            expected_size = 4;
            break;

        case Type::Code::Int64:
        case Type::Code::UInt64:
        case Type::Code::Float64:
        case Type::Code::DateTime64:
        case Type::Code::IPv6:
        case Type::Code::Decimal64:
            expected_size = 8;
            break;

        case Type::Code::String:
        case Type::Code::FixedString:
            // value can be of any size
            return;

        case Type::Code::Array:
        case Type::Code::Nullable:
        case Type::Code::Tuple:
        case Type::Code::LowCardinality:
            throw std::runtime_error("Unsupported type in ItemView: " + std::to_string(static_cast<int>(type)));

        case Type::Code::UUID:
        case Type::Code::Int128:
        case Type::Code::Decimal:
        case Type::Code::Decimal128:
            expected_size = 16;
            break;

        default:
            throw std::runtime_error("Unknon type code:" + std::to_string(static_cast<int>(type)));
    }

    if (expected_size != static_cast<int>(data.size())) {
        throw std::runtime_error("Value size mismatch for type "
                + std::to_string(static_cast<int>(type)) + " expected: "
                + std::to_string(expected_size) + ", got: " + std::to_string(data.size()));
    }
}

}
