/** Collection of integration tests that validate TLS-connectivity to a CH server
 */
#include "readonly_client_ut.h"
#include "connection_failed_client_ut.h"

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

INSTANTIATE_TEST_CASE_P(
    RemoteTLS, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType {
        ClientOptions()
            .SetHost("github.demo.trial.altinity.cloud")
            .SetPort(9440)
            .SetUser("demo")
            .SetPassword("demo")
            .SetDefaultDatabase("default")
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            // On Ubuntu 20.04 /etc/ssl/certs is a default directory with the CA files
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetPathToCADirectory("/etc/ssl/certs")),
        QUERIES
    }
));

#if defined(__linux__)
// On Ubuntu 20.04 /etc/ssl/certs is a default directory with the CA files
const auto DEAFULT_CA_DIRECTORY_PATH = "/etc/ssl/certs";
#elif defined(__APPLE__)
#elif defined(_win_)
#endif

INSTANTIATE_TEST_CASE_P(
    Remote_GH_API_TLS, ReadonlyClientTest,
    ::testing::Values(ReadonlyClientTest::ParamType {
        ClientOptions()
            .SetHost("gh-api.clickhouse.tech")
            .SetPort(9440)
            .SetUser("explorer")
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                    .SetPathToCADirectory(DEAFULT_CA_DIRECTORY_PATH)),
        QUERIES
    }
));

INSTANTIATE_TEST_CASE_P(
    Remote_GH_API_TLS_no_CA, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType {
        ClientOptions()
            .SetHost("gh-api.clickhouse.tech")
            .SetPort(9440)
            .SetUser("explorer")
            .SetSendRetries(1)
            .SetPingBeforeQuery(true)
            .SetCompressionMethod(CompressionMethod::None)
            .SetSSLOptions(ClientOptions::SSLOptions()
                     .SetUseDefaultCALocations(false)),
        "X509_v error: 20 unable to get local issuer certificate"
    }
));

INSTANTIATE_TEST_CASE_P(
    Remote_GH_API_TLS_wrong_TLS_version, ConnectionFailedClientTest,
    ::testing::Values(ConnectionFailedClientTest::ParamType {
        ClientOptions()
            .SetHost("gh-api.clickhouse.tech")
            .SetPort(9440)
            .SetUser("explorer")
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
//INSTANTIATE_TEST_CASE_P(
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

//INSTANTIATE_TEST_CASE_P(
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
