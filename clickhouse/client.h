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
    std::string default_database = "default";
    /// User name.
    std::string user = "default";
    /// Access password.
    std::string password = "";

    inline ClientOptions& SetHost(const std::string& value) {
        host = value;
        return *this;
    }

    inline ClientOptions& SetPort(const int value) {
        port = value;
        return *this;
    }
};

/**
 *
 */
class Client {
public:
     Client();
     Client(const ClientOptions& opts);
    ~Client();

    /// Intends for execute arbitrary queries.
    void Execute(const Query& query);

    /// Intends for execute select queries.  Data will be returned with
    /// one or more call of \p cb.
    void Select(const std::string& query, SelectCallback cb);

    /// Intends for insert block of data into a table \p table_name.
    void Insert(const std::string& table_name, const Block& block);

    /// Ping server for aliveness.
    void Ping();

private:
    ClientOptions options_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
