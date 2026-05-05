#pragma once

#include "block.h"
#include "query.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace clickhouse {

struct AsyncClientOptions {
    std::string host;
    std::uint16_t port{9000};
    std::string database{"default"};
    std::string user{"default"};
    std::string password;

    // Buffering / backpressure
    std::size_t max_inflight_requests{64};
    std::size_t max_inflight_bytes{16 * 1024 * 1024};
    std::size_t inbox_ring_bytes{1 * 1024 * 1024};

    // Timeouts / failure handling
    std::chrono::milliseconds connect_timeout{2000};
    std::chrono::milliseconds stall_timeout{2000};
    std::chrono::milliseconds cooldown{5000};
};

enum class EnqueueResult : std::uint8_t {
    queued,
    dropped,
    disabled,
    not_connected,
};

struct PollResult {
    bool progressed{false};
    bool connected{false};
    std::size_t bytes_sent{0};
    std::size_t bytes_recv{0};
    std::size_t requests_completed{0};
    std::size_t requests_failed{0};
};

class AsyncClient {
public:
    explicit AsyncClient(AsyncClientOptions options);
    ~AsyncClient();

    // Cold-path operations
    void start_connect();
    void close();
    bool connected() const noexcept;
    bool disabled() const noexcept;

    // Hot-path operations (must not block)
    EnqueueResult enqueue_insert(
        std::string_view table,
        const Block& block,
        std::string_view query_id = Query::default_query_id);

    // Make bounded progress; never blocks.
    PollResult poll(
        std::chrono::steady_clock::time_point now,
        std::chrono::nanoseconds budget);

    // Observability
    std::size_t inflight_requests() const noexcept;
    std::size_t inflight_bytes() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clickhouse

