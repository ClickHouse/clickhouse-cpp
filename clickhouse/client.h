#pragma once

#include "query.h"
#include "exceptions.h"

#include "columns/array.h"
#include "columns/date.h"
#include "columns/decimal.h"
#include "columns/enum.h"
#include "columns/ip4.h"
#include "columns/ip6.h"
#include "columns/lowcardinality.h"
#include "columns/nullable.h"
#include "columns/numeric.h"
#include "columns/string.h"
#include "columns/tuple.h"
#include "columns/uuid.h"

#include <chrono>
#include <memory>
#include <ostream>
#include <string>

namespace clickhouse {

struct ServerInfo {
    std::string name;
    std::string timezone;
    std::string display_name;
    uint64_t    version_major;
    uint64_t    version_minor;
    uint64_t    version_patch;
    uint64_t    revision;
};

/// Methods of block compression.
enum class CompressionMethod {
    None    = -1,
    LZ4     =  1,
};

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
    DECLARE_FIELD(port, unsigned int, SetPort, 9000);

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

    /// Ping server every time before execute any query.
    DECLARE_FIELD(ping_before_query, bool, SetPingBeforeQuery, false);
    /// Count of retry to send request to server.
    DECLARE_FIELD(send_retries, unsigned int, SetSendRetries, 1);
    /// Amount of time to wait before next retry.
    DECLARE_FIELD(retry_timeout, std::chrono::seconds, SetRetryTimeout, std::chrono::seconds(5));

    /// Compression method.
    DECLARE_FIELD(compression_method, CompressionMethod, SetCompressionMethod, CompressionMethod::None);

    /// TCP Keep alive options
    DECLARE_FIELD(tcp_keepalive, bool, TcpKeepAlive, false);
    DECLARE_FIELD(tcp_keepalive_idle, std::chrono::seconds, SetTcpKeepAliveIdle, std::chrono::seconds(60));
    DECLARE_FIELD(tcp_keepalive_intvl, std::chrono::seconds, SetTcpKeepAliveInterval, std::chrono::seconds(5));
    DECLARE_FIELD(tcp_keepalive_cnt, unsigned int, SetTcpKeepAliveCount, 3);

#undef DECLARE_FIELD
};

std::ostream& operator<<(std::ostream& os, const ClientOptions& options);

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

    /// Executes a select query which can be canceled by returning false from
    /// the data handler function \p cb.
    void SelectCancelable(const std::string& query, SelectCancelableCallback cb);

    /// Alias for Execute.
    void Select(const Query& query);

    /// Intends for insert block of data into a table \p table_name.
    void Insert(const std::string& table_name, const Block& block);

    /// Ping server for aliveness.
    void Ping();

    /// Reset connection with initial params.
    void ResetConnection();

    const ServerInfo& GetServerInfo() const;

private:
    ClientOptions options_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
