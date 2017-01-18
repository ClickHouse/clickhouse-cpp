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
     Client(const ClientOptions& opts, QueryEvents* events);
    ~Client();

    void ExecuteQuery(const std::string& query);

    /// Insert block of data to a table \p table_name.
    void Insert(const std::string& table_name, const Block& block);

private:
    ClientOptions options_;
    QueryEvents* events_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
