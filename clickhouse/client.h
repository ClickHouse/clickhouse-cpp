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

#if defined(WITH_OPENSSL)
typedef struct ssl_ctx_st SSL_CTX;
#endif

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
#define DECLARE_FIELD(name, type, setter, default_value) \
    type name = default_value; \
    inline auto & setter(const type& value) { \
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

    // TCP options
    DECLARE_FIELD(tcp_nodelay, bool, TcpNoDelay, true);

    /** It helps to ease migration of the old codebases, which can't afford to switch
    * to using ColumnLowCardinalityT or ColumnLowCardinality directly,
    * but still want to benefit from smaller on-wire LowCardinality bandwidth footprint.
    *
    * @see LowCardinalitySerializationAdaptor, CreateColumnByType
    */
    DECLARE_FIELD(backward_compatibility_lowcardinality_as_wrapped_column, bool, SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn, true);

    /** Set max size data to compress if compression enabled.
     *
     *  Allows choosing tradeoff betwen RAM\CPU:
     *  - Lower value reduces RAM usage, but slightly increases CPU usage.
     *  - Higher value increases RAM usage but slightly decreases CPU usage.
     *
     *  Default is 0, use natural implementation-defined chunk size.
     */
    DECLARE_FIELD(max_compression_chunk_size, unsigned int, SetMaxCompressionChunkSize, 65535);

#if defined(WITH_OPENSSL)
    struct SSLOptions {
        bool use_ssl = true; // not expected to be set manually.

        /** There are two ways to configure an SSL connection:
         *  - provide a pre-configured SSL_CTX, which is not modified and not owned by the Client.
         *  - provide a set of options and allow the Client to create and configure SSL_CTX by itself.
         */

        /** Pre-configured SSL-context for SSL-connection.
         *  If NOT null client DONES NOT take ownership of context and it must be valid for client lifetime.
         *  If null client initlaizes OpenSSL and creates his own context, initializes it using
         *  other options, like path_to_ca_files, path_to_ca_directory, use_default_ca_locations, etc.
         */
        SSL_CTX * ssl_context = nullptr;
        auto & SetExternalSSLContext(SSL_CTX * new_ssl_context) {
            ssl_context = new_ssl_context;
            return *this;
        }

        /** Means to validate the server-supplied certificate against trusted Certificate Authority (CA).
         *  If no CAs are configured, the server's identity can't be validated, and the Client would err.
         *  See https://www.openssl.org/docs/man1.1.1/man3/SSL_CTX_set_default_verify_paths.html
        */
        /// Load deafult CA certificates from deafult locations.
        DECLARE_FIELD(use_default_ca_locations, bool, SetUseDefaultCALocations, true);
        /// Path to the CA files to verify server certificate, may be empty.
        DECLARE_FIELD(path_to_ca_files, std::vector<std::string>, SetPathToCAFiles, {});
        /// Path to the directory with CA files used to validate server certificate, may be empty.
        DECLARE_FIELD(path_to_ca_directory, std::string, SetPathToCADirectory, "");

        /** Min and max protocol versions to use, set with SSL_CTX_set_min_proto_version and SSL_CTX_set_max_proto_version
         *  for details see https://www.openssl.org/docs/man1.1.1/man3/SSL_CTX_set_min_proto_version.html
         */
        DECLARE_FIELD(min_protocol_version, int, SetMinProtocolVersion, DEFAULT_VALUE);
        DECLARE_FIELD(max_protocol_version, int, SetMaxProtocolVersion, DEFAULT_VALUE);

        /** Options to be set with SSL_CTX_set_options,
         * for details see https://www.openssl.org/docs/man1.1.1/man3/SSL_CTX_set_options.html
        */
        DECLARE_FIELD(context_options, int, SetContextOptions, DEFAULT_VALUE);

        /** Use SNI at ClientHello and verify that certificate is issued to the hostname we are trying to connect to
         */
        DECLARE_FIELD(use_sni, bool, SetUseSNI, true);

        static const int DEFAULT_VALUE = -1;
    };

    // By default SSL is turned off, hence the `{false}`
    DECLARE_FIELD(ssl_options, SSLOptions, SetSSLOptions, {false});
#endif

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
    const ClientOptions options_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
