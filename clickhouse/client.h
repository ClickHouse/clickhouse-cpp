#pragma once

#include "query.h"
#include "exceptions.h"

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/tuple.h"

#include <memory>
#include <string>

namespace clickhouse {

struct ClientOptions {
#define DECLARE_FIELD(name, type, setter, default) \
    type name = default; \
    inline ClientOptions& setter(const type& value) { \
        name = value; \
        return *this; \
    }

    /// Hostname of the server.
    DECLARE_FIELD(host, std::string, SetHost, std::string());
    /// Service port.
    DECLARE_FIELD(port, int, SetPort, 9000);

    /// Default database.
    DECLARE_FIELD(default_database, std::string, SetDefaultDatabase, "default");
    /// User name.
    DECLARE_FIELD(user, std::string, SetUser, "default");
    /// Access password.
    DECLARE_FIELD(password, std::string, SetPassword, std::string());

    /// By default all exceptions received during query execution will be
    /// passed to OnException handler.  Set rethrow_exceptions to true to
    /// enable throwing exceptions with standard c++ exception mechanism.
    DECLARE_FIELD(rethrow_exceptions, bool, SetRethrowException, true);

#undef DECLARE_FIELD
};

/**
 *
 */
class Client {
public:
     Client(const ClientOptions& opts);
    ~Client();

    /// Intends for execute arbitrary queries.
    void Execute(const Query& query);

    /// Intends for execute select queries.  Data will be returned with
    /// one or more call of \p cb.
    void Select(const std::string& query, SelectCallback cb);

    /// Alias for Execute.
    void Select(const Query& query);

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
