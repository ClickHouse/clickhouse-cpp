#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace clickhouse {

class OutputStream {
public:
    virtual ~OutputStream()
    { }

    inline void Flush() {
        DoFlush();
    }

    inline void Write(const void* data, size_t len) {
        DoWrite(data, len);
    }

protected:
    virtual void DoFlush() { }

    virtual void DoWrite(const void* data, size_t len) = 0;
};


class ZeroCopyOutput : public OutputStream {
public:
    inline size_t Next(void** data, size_t size) {
        return DoNext(data, size);
    }

protected:
    // Obtains a buffer into which data can be written.  Any data written
    // into this buffer will eventually (maybe instantly, maybe later on)
    // be written to the output.
    virtual size_t DoNext(void** data, size_t len) = 0;

    void DoWrite(const void* data, size_t len) override;
};


/**
 * A ZeroCopyOutput stream backed by an in-memory array of bytes.
 */
class ArrayOutput : public ZeroCopyOutput {
public:
     ArrayOutput(void* buf, size_t len);
    ~ArrayOutput() override;

    /// Number of bytes available in the stream.
    inline size_t Avail() const noexcept {
        return end_ - buf_;
    }

    /// Current write position in the memory block used by this stream.
    inline const uint8_t* Data() const noexcept {
        return buf_;
    }

    /// Whether there is more space in the stream.
    inline bool Exhausted() const noexcept {
        return !Avail();
    }

    /// Initializes this stream with a new memory block.
    inline void Reset(void* buf, size_t len) noexcept {
        buf_ = static_cast<uint8_t*>(buf);
        end_ = buf_ + len;
    }

protected:
    size_t DoNext(void** data, size_t len) override;

private:
    uint8_t* buf_;
    uint8_t* end_;
};


class BufferedOutput : public ZeroCopyOutput {
public:
     BufferedOutput(OutputStream* slave, size_t buflen = 8192);
    ~BufferedOutput() override;

protected:
    void DoFlush() override;
    size_t DoNext(void** data, size_t len) override;
    void DoWrite(const void* data, size_t len) override;

private:
    OutputStream* const slave_;
    std::vector<uint8_t> buffer_;
    ArrayOutput array_output_;
};

}
