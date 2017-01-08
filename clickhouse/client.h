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

    /// Default database.
    std::string default_database = "system";
    /// User name.
    std::string user = "default";
    /// Access password.
    std::string password = "";
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
    ClientOptions options_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
