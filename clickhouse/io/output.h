#pragma once

namespace clickhouse {
namespace io {

class OutputStream {
public:
    virtual ~OutputStream()
    { }

    inline void Write(const void* data, size_t len) {
        DoWrite(data, len);
    }

protected:
    virtual void DoWrite(const void* data, size_t len) = 0;
};

}
}
