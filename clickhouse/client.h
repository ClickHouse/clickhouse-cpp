#pragma once

#include "query.h"

#include <memory>
#include <string>

namespace clickhouse {

struct ClientOptions {
    /// Hostname of the server.
    std::string host;
    /// Service port.
    int port = 9000;

    ClientOptions& SetHost(const std::string& val) {
        host = val;
        return *this;
    }

    ClientOptions& SetPort(const int val) {
        port = val;
        return *this;
    }
};

/**
 *
 */
class Client {
public:
    Client();
    explicit Client(const ClientOptions& opts);
    explicit Client(const std::string& host, int port = 9000);
    ~Client();

    void Connect();

    void ExecuteQuery(const std::string& query, QueryEvents* events);

private:
    const std::string host_;
    const int port_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
