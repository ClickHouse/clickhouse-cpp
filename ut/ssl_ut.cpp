/** Collection of integration tests that validate TLS-connectivity to a CH server
 */
#include <openssl/ssl2.h>
#include <openssl/ssl3.h>
#include <openssl/tls1.h>

#include <cerrno>

#include "connection_failed_client_test.h"
#include "readonly_client_test.h"
#include "tcp_server.h"
#include "utils.h"

namespace {
    using namespace clickhouse;

    const auto QUERIES = std::vector<std::string> {
        "SELECT version()",
        "SELECT fqdn()",
        "SELECT blockSize()",
        "SELECT uptime()",
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
            .SetHost(           getEnvOrDefault("CLICKHOUSE_SECURE_HOST",     "play.clickhouse.com"))
            .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_SECURE_PORT",     "9440"))
            .SetUser(           getEnvOrDefault("CLICKHOUSE_SECURE_USER",     "explorer"))
            .SetPassword(       getEnvOrDefault("CLICKHOUSE_SECURE_PASSWORD", ""))
            .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_SECURE_DB",       "default"))
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetPathToCADirectory(DEFAULT_CA_DIRECTORY_PATH)),
        QUERIES
    }
));


const auto ClickHouseExplorerConfig = ClientOptions()
        .SetHost(           getEnvOrDefault("CLICKHOUSE_SECURE2_HOST",     "play.clickhouse.com"))
        .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_SECURE2_PORT",     "9440"))
        .SetUser(           getEnvOrDefault("CLICKHOUSE_SECURE2_USER",     "explorer"))
        .SetPassword(       getEnvOrDefault("CLICKHOUSE_SECURE2_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_SECURE2_DB",       "default"))
        .SetSendRetries(1)
        .SetPingBeforeQuery(true)
        .SetCompressionMethod(CompressionMethod::None);


INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType {
        ClientOptions(ClickHouseExplorerConfig)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetPathToCADirectory(DEFAULT_CA_DIRECTORY_PATH)),
        QUERIES
    }
));

// For some reasons doesn't work on MacOS.
// Looks like `VerifyCAPath` has no effect, while parsing and setting value works.
// Also for some reason SetPathToCADirectory() + SSL_CTX_load_verify_locations() works.
#if !defined(__APPLE__)
TEST(OpenSSLConfiguration, DISABLED_ValidValues) {
    // Verify that Client with valid configuration set via SetConfiguration is able to connect.

    EXPECT_NO_THROW(
        Client(ClientOptions(ClickHouseExplorerConfig)
                .SetSSLOptions(ClientOptions::SSLOptions()
                        .SetConfiguration({
                                {"VerifyCAPath", DEFAULT_CA_DIRECTORY_PATH},
                                {"MinProtocol", "TLSv1.3"},
                                {"VerifyMode", "Peer"},
                                {"no_comp"} // shorthand for command with no value
                        })
        ))
    );
}
#endif

TEST(OpenSSLConfiguration, InValidValues) {
    // Verify that invalid options cause throwing exception.
    std::vector<ClientOptions::SSLOptions::CommandAndValue> configurations = {
        {"VerifyCAPath", std::nullopt},              // missing required value
        {"VerifyCAPath"},                            // missing required value, shorthand
        {"MinProtocol", "NOT A STRING"},             // invalid value
        {"MinProtocol", ""},                         // invalid value
        {"unknownCommand"},                          // invalid command, shorthand
        {"unknownCommand", "with unexpected value"}, // invalid command + some value
        {"", std::nullopt},                          // empty command with no value
        {""},                                        // empty command with no value
        {"no_comp", "unexpected value"}              // value for command that doesn't require a value.
    };

    // Unfortunately, there is no way of checking configuration validity without
    // creating a Client and pointing that client to a working CH server.
    // So we mix valid and invalid configurations by providing the correct
    // server config but incorrect single Command-Value pair individually.
    for (const auto & cv : configurations) {
        EXPECT_ANY_THROW(
            Client(ClientOptions(ClickHouseExplorerConfig)
                    .SetSSLOptions(ClientOptions::SSLOptions()
                            .SetConfiguration({cv})
            ));
        ) << "On command \'" << cv.command << "\' and value: " << (cv.value ? *cv.value : "<EMPTY>");
    }
}

TEST(OpenSSLConnection, HandshakeIsBoundedByConnectTimeout) {
    // The kernel completes the TCP handshake from the listen backlog, but nobody ever
    // answers the ClientHello. Each endpoint must give up after the connect timeout and
    // rotation must move on to the next one; the receive timeout is only a hang guard.
    const int first_port  = 19980;
    const int second_port = 19981;
    LocalTcpServer first(first_port);
    LocalTcpServer second(second_port);
    first.start();
    second.start();

    const auto connect_timeout = std::chrono::milliseconds(500);
    const auto start           = std::chrono::steady_clock::now();
    try {
        Client client(ClientOptions()
                          .SetEndpoints({{"127.0.0.1", first_port}, {"127.0.0.1", second_port}})
                          .SetSendRetries(1)
                          .SetConnectionConnectTimeout(connect_timeout)
                          .SetConnectionRecvTimeout(std::chrono::seconds(10))
                          .SetSSLOptions(ClientOptions::SSLOptions().SetSkipVerification(true)));
        FAIL() << "TLS handshake completed against a peer that never answered";
    } catch (const std::system_error& e) {
#if defined(_win_)
        EXPECT_EQ(WSAETIMEDOUT, e.code().value()) << e.what();
#else
        EXPECT_EQ(ETIMEDOUT, e.code().value()) << e.what();
#endif
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_GE(elapsed, 2 * connect_timeout);
    EXPECT_LT(elapsed, 2 * connect_timeout + std::chrono::seconds(2));
}

//INSTANTIATE_TEST_SUITE_P(
//    Remote_GH_API_TLS_WithStringConfig, ReadonlyClientTest,
//    ::testing::Values(ReadonlyClientTest::ParamType {
//        ClientOptions(ClickHouseExplorerConfig)
//            .SetSSLOptions(ClientOptions::SSLOptions()
//                    .SetConfiguration({{"", DEFAULT_CA_DIRECTORY_PATH}})
//        QUERIES
//    }
//));

INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS_no_CA, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType {
        ClientOptions(ClickHouseExplorerConfig)
            .SetSSLOptions(ClientOptions::SSLOptions()
                     .SetUseDefaultCALocations(false)
                     .SetSkipVerification(true)), // No CA loaded, but verification is skipped
        {"SELECT 1;"}
    }
));

INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS_no_CA, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType {
        ClientOptions(ClickHouseExplorerConfig)
            .SetSSLOptions(ClientOptions::SSLOptions()
                     .SetUseDefaultCALocations(false)),
        ExpectingException{"X509_v error: 20 unable to get local issuer certificate"}
    }
));

INSTANTIATE_TEST_SUITE_P(
    Remote_GH_API_TLS_wrong_TLS_version, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType {
        ClientOptions(ClickHouseExplorerConfig)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetUseDefaultCALocations(false)
                    .SetMaxProtocolVersion(SSL3_VERSION)),
        ExpectingException{"no protocols available"}
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
