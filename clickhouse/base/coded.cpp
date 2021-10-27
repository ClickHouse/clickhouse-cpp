#include "coded.h"

#include <memory.h>

namespace clickhouse {

//static const int MAX_VARINT_BYTES = 10;

//CodedInputStream::CodedInputStream(ZeroCopyInput* input)
//    : input_(input)
//{
//}

//bool CodedInputStream::ReadRaw(void* buffer, size_t size) {
//    uint8_t* p = static_cast<uint8_t*>(buffer);

//    while (size > 0) {
//        const void* ptr;
//        size_t len = input_->Next(&ptr, size);

//        memcpy(p, ptr, len);

//        p += len;
//        size -= len;
//    }

//    return true;
//}

}
