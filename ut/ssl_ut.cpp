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
            .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_SECURE_PORT",     "9440"))
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
