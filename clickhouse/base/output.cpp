#include "output.h"

#include <algorithm>
#include <memory.h>

namespace clickhouse {

void ZeroCopyOutput::DoWrite(const void* data, size_t len) {
    while (len > 0) {
        void* ptr;
        size_t result = DoNext(&ptr, len);

        if (result) {
            memcpy(ptr, data, result);
            len -= result;
            data = static_cast<const uint8_t*>(data) + result;
        } else {
            break;
        }
    }
}


ArrayOutput::ArrayOutput(void* buf, size_t len)
    : buf_(static_cast<uint8_t*>(buf))
    , end_(buf_ + len)
{
}

ArrayOutput::~ArrayOutput() = default;

size_t ArrayOutput::DoNext(void** data, size_t len) {
    len = std::min(len, Avail());

    *data = buf_;
    buf_ += len;

    return len;
}


BufferedOutput::BufferedOutput(OutputStream* slave, size_t buflen)
    : slave_(slave)
    , buffer_(buflen)
    , array_output_(buffer_.data(), buflen)
{
}

BufferedOutput::~BufferedOutput() {
    Flush();
}

void BufferedOutput::DoFlush() {
    if (array_output_.Data() != buffer_.data()) {
        slave_->Write(buffer_.data(), array_output_.Data() - buffer_.data());
        slave_->Flush();

        array_output_.Reset(buffer_.data(), buffer_.size());
    }
}

size_t BufferedOutput::DoNext(void** data, size_t len) {
    if (array_output_.Avail() < len) {
        Flush();
    }

    return array_output_.Next(data, len);

}

void BufferedOutput::DoWrite(const void* data, size_t len) {
    if (array_output_.Avail() < len) {
        Flush();

        if (len > buffer_.size() / 2) {
            slave_->Write(data, len);
            return;
        }
    }

    array_output_.Write(data, len);
}

}
