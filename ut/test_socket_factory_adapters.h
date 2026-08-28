#pragma once

#include "clickhouse/base/socket.h"

#include <chrono>
#include <memory>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

namespace clickhouse {

/** Records requested endpoints and optionally fails one matching connection attempt.
 *
 * Successful attempts are redirected to actual_endpoint, allowing tests to exercise
 * failover between distinct logical endpoints using a single reachable server. Setting
 * fail_endpoint makes the next matching attempt throw connection_refused and then
 * clears the value. The wrapped factory must outlive the adapter.
 */
struct FailOnceSocketFactoryAdapter : public SocketFactory {
    SocketFactory & socket_factory;
    Endpoint actual_endpoint;
    std::vector<Endpoint> connect_requests{};
    std::optional<Endpoint> fail_endpoint{};

    FailOnceSocketFactoryAdapter(SocketFactory & socket_factory,
                                 Endpoint actual_endpoint)
        : socket_factory(socket_factory)
        , actual_endpoint(std::move(actual_endpoint))
    {}

    std::unique_ptr<SocketBase> connect(const ClientOptions& opts,
                                        const Endpoint& endpoint) override {
        connect_requests.push_back(endpoint);

        if (fail_endpoint && fail_endpoint.value() == endpoint) {
            fail_endpoint.reset();
            throw std::system_error(std::make_error_code(std::errc::connection_refused));
        }

        return socket_factory.connect(opts, actual_endpoint);
    }

    void SetFailEndpoint(std::optional<Endpoint> endpoint) {
        fail_endpoint = std::move(endpoint);
    }

    const std::vector<Endpoint> & ConnectRequests() const {
        return connect_requests;
    }

    void ClearConnectRequests() {
        connect_requests.clear();
    }

    void sleepFor(const std::chrono::milliseconds& duration) override {
        socket_factory.sleepFor(duration);
    }
};

} // namespace clickhouse
