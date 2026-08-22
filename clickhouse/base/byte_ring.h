#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace clickhouse::internal {

class ByteRing {
public:
    explicit ByteRing(std::size_t capacity)
        : buffer_(capacity)
    {}

    std::size_t capacity() const noexcept { return buffer_.size(); }
    std::size_t size() const noexcept { return size_; }
    std::size_t available() const noexcept { return capacity() - size_; }

    void clear() noexcept {
        head_ = 0;
        size_ = 0;
    }

    struct Span {
        std::uint8_t* data{nullptr};
        std::size_t size{0};
    };

    struct ConstSpan {
        const std::uint8_t* data{nullptr};
        std::size_t size{0};
    };

    Span write_span() noexcept {
        if (available() == 0 || capacity() == 0) {
            return {};
        }
        const std::size_t tail = (head_ + size_) % capacity();
        const bool wrapped = tail < head_;
        const std::size_t contiguous = wrapped ? (head_ - tail) : (capacity() - tail);
        return {buffer_.data() + tail, std::min(contiguous, available())};
    }

    void commit_write(std::size_t n) noexcept {
        n = std::min(n, write_span().size);
        size_ += n;
    }

    ConstSpan read_span() const noexcept {
        if (size_ == 0 || capacity() == 0) {
            return {};
        }
        const std::size_t contiguous = std::min(size_, capacity() - head_);
        return {buffer_.data() + head_, contiguous};
    }

    void consume_read(std::size_t n) noexcept {
        n = std::min(n, size_);
        if (n == 0) {
            return;
        }
        head_ = (head_ + n) % capacity();
        size_ -= n;
    }

    std::size_t write(const void* data, std::size_t len) {
        const auto* p = static_cast<const std::uint8_t*>(data);
        std::size_t written = 0;
        while (written < len && available() > 0) {
            const auto span = write_span();
            const std::size_t n = std::min(span.size, len - written);
            if (n == 0) {
                break;
            }
            std::memcpy(span.data, p + written, n);
            commit_write(n);
            written += n;
        }
        return written;
    }

    std::size_t read(void* data, std::size_t len) {
        auto* p = static_cast<std::uint8_t*>(data);
        std::size_t read_total = 0;
        while (read_total < len && size_ > 0) {
            const auto span = read_span();
            const std::size_t n = std::min(span.size, len - read_total);
            if (n == 0) {
                break;
            }
            std::memcpy(p + read_total, span.data, n);
            consume_read(n);
            read_total += n;
        }
        return read_total;
    }

    std::size_t discard(std::size_t len) noexcept {
        const std::size_t n = std::min(len, size_);
        consume_read(n);
        return n;
    }

private:
    std::vector<std::uint8_t> buffer_;
    std::size_t head_{0};
    std::size_t size_{0};
};

}  // namespace clickhouse::internal
