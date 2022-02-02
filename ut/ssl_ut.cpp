/** Collection of integration tests that validate TLS-connectivity to a CH server
 */
#include "readonly_client_test.h"
#include "connection_failed_client_test.h"
#include "utils.h"

#include <openssl/tls1.h>
#include <openssl/ssl2.h>
#include <openssl/ssl3.h>

namespace {
    using namespace clickhouse;

    const auto QUERIES = std::vector<std::string> {
        "SELECT version()",
        "SELECT fqdn()",
        "SELECT buildId()",
        "SELECT uptime()",
        "SELECT filesystemFree()",
        "SELECT now()"
    };
}

#if defined(__linux__)
// On Ubuntu 20.04 /etc/ssl/certs is a default directory with the CA files
const auto DEFAULT_CA_DIRECTORY_PATH = "/etc/ssl/certs";
#elif defined(__APPLE__)
// On macOS we will rely on Homebrew's OpenSSL installation
const auto DEFAULT_CA_DIRECTORY_PATH = "/usr/local/etc/openssl@1.1/cert.pem";
#elif defined(_win_)
// DEBUG ME: mingw - was not able to make it work. Every time it ends with exception:
// "Failed to verify SSL connection, X509_v error: 20 unable to get local issuer certificate"
const auto DEFAULT_CA_DIRECTORY_PATH = "/mingw64/ssl/cert.pem";
// const auto DEFAULT_CA_DIRECTORY_PATH = "/mingw64/ssl/certs";
// const auto DEFAULT_CA_DIRECTORY_PATH = "/mingw64/ssl/certs/ca-bundle.crt";
#endif

INSTANTIATE_TEST_SUITE_P(
    RemoteTLS, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType {
        ClientOptions()
            .SetHost(           getEnvOrDefault("CLICKHOUSE_SECURE_HOST",     "github.demo.trial.altinity.cloud"))
            .SetPort( std::stoi(getEnvOrDefault("CLICKHOUSE_SECURE_PORT",     "9440")))
            .SetUser(           getEnvOrDefault("CLICKHOUSE_SECURE_USER",     "demo"))
            .SetPassword(       getEnvOrDefault("CLICKHOUSE_SECURE_PASSWORD", "demo"))
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_SECURE_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetPathToCADirectory(DEFAULT_CA_DIRECTORY_PATH)),
        QUERIES
    }
));

INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType {
        ClientOptions()
            .SetHost(           getEnvOrDefault("CLICKHOUSE_SECURE2_HOST",     "gh-api.clickhouse.tech"))
            .SetPort( std::stoi(getEnvOrDefault("CLICKHOUSE_SECURE2_PORT",     "9440")))
            .SetUser(           getEnvOrDefault("CLICKHOUSE_SECURE2_USER",     "explorer"))
            .SetPassword(       getEnvOrDefault("CLICKHOUSE_SECURE2_PASSWORD", ""))
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_SECURE2_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetPathToCADirectory(DEFAULT_CA_DIRECTORY_PATH)),
        QUERIES
    }
));

INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS_no_CA, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType {
        ClientOptions()
            .SetHost(           getEnvOrDefault("CLICKHOUSE_SECURE2_HOST",     "gh-api.clickhouse.tech"))
            .SetPort( std::stoi(getEnvOrDefault("CLICKHOUSE_SECURE2_PORT",     "9440")))
            .SetUser(           getEnvOrDefault("CLICKHOUSE_SECURE2_USER",     "explorer"))
            .SetPassword(       getEnvOrDefault("CLICKHOUSE_SECURE2_PASSWORD", ""))
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_SECURE2_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                     .SetUseDefaultCALocations(false)),
        "X509_v error: 20 unable to get local issuer certificate"
    }
));

INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS_wrong_TLS_version, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType {
        ClientOptions()
            .SetHost(           getEnvOrDefault("CLICKHOUSE_SECURE2_HOST",     "gh-api.clickhouse.tech"))
            .SetPort( std::stoi(getEnvOrDefault("CLICKHOUSE_SECURE2_PORT",     "9440")))
            .SetUser(           getEnvOrDefault("CLICKHOUSE_SECURE2_USER",     "explorer"))
            .SetPassword(       getEnvOrDefault("CLICKHOUSE_SECURE2_PASSWORD", ""))
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_SECURE2_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetUseDefaultCALocations(false)
                    .SetMaxProtocolVersion(SSL3_VERSION)),
        "no protocols available"
    }
));

//// Special test that require properly configured TLS-enabled version of CH running locally
//INSTANTIATE_TEST_SUITE_P(
//    LocalhostTLS_None, ReadonlyClientTest,
//    ::testing::Values(std::tuple<ClientOptions, std::vector<std::string> > {
//        ClientOptions()
//            .SetHost("127.0.0.1")
//            .SetPort(9440)
//            .SetUser("default")
//            .SetPingBeforeQuery(true)
//            .SetCompressionMethod(CompressionMethod::None)
//            .SetSSLOptions(ClientOptions::SSLOptions()
//                    .SetPathToCADirectory("./CA/")
//                    .SetUseSNI(false)),
//        QUERIES
//    }
//));

//INSTANTIATE_TEST_SUITE_P(
//    LocalhostTLS_LZ4, ReadonlyClientTest,
//    ::testing::Values(std::tuple<ClientOptions, std::vector<std::string> > {
//        ClientOptions()
//            .SetHost("127.0.0.1")
//            .SetPort(9440)
//            .SetUser("default")
//            .SetPingBeforeQuery(true)
//            .SetCompressionMethod(CompressionMethod::LZ4)
//            .SetSSLOptions(ClientOptions::SSLOptions()
//                    .SetPathToCADirectory("./CA/")
//                    .SetUseSNI(false)),
//        QUERIES
//    }
//));
