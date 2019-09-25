#include "output.h"

#include <algorithm>
#include <assert.h>
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


BufferOutput::BufferOutput(Buffer* buf)
    : buf_(buf)
    , pos_(0)
{
    assert(buf_);
}

BufferOutput::~BufferOutput()
{ }

size_t BufferOutput::DoNext(void** data, size_t len) {
    if (pos_ + len > buf_->size()) {
        buf_->resize(pos_ + len);
    }

    *data = buf_->data() + pos_;
    pos_ += len;

    return len;
}


BufferedOutput::BufferedOutput(OutputStream* slave, size_t buflen)
    : slave_(slave)
    , buffer_(buflen)
    , array_output_(buffer_.data(), buflen)
{
}

BufferedOutput::~BufferedOutput() {
    try
    {
        Flush();
    }
    catch (...)
    {
        // That means we've failed to flush some data e.g. to the socket,
        // but there is nothing we can do at this point (can't bring the socket back),
        // and throwing in destructor is really a bad idea.
        // The best we can do is to log the error and ignore it, but currently there is no logging subsystem.
    }
}

void BufferedOutput::Reset() {
    array_output_.Reset(buffer_.data(), buffer_.size());
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
