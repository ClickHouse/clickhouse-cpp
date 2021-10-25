#include "sslsocket.h"

#include <stdexcept>

#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <openssl/err.h>
#include <openssl/asn1.h>

#include <iostream>

namespace {

std::string getCertificateInfo(X509* cert)
{
    std::unique_ptr<BIO, decltype(&BIO_free)> mem_bio(BIO_new(BIO_s_mem()), &BIO_free);
    X509_print(mem_bio.get(), cert);
    char * data = nullptr;
    size_t len = BIO_get_mem_data(mem_bio.get(), &data);

    return std::string(data, len);
}

void throwSSLError(SSL * ssl, int error, const char * location, const char * statement) {
    const auto detail_error = ERR_get_error();
    auto reason = ERR_reason_error_string(detail_error);
    reason = reason ? reason : "Unknown SSL error";

    std::string reason_str = reason;
    if (ssl) {
        if (auto ssl_session = SSL_get_session(ssl))
            if (auto server_certificate = SSL_SESSION_get0_peer(ssl_session))
                reason_str += "\nServer certificate: " + getCertificateInfo(server_certificate);
    }

    std::cerr << "!!! SSL error at " << location
              << "\n\tcaused by " << statement
              << "\n\t: "<< reason_str << "(" << error << ")"
              << "\n\t last err: " << ERR_peek_last_error()
              << std::endl;

    throw std::runtime_error(std::string("OpenSSL error: ") + std::to_string(error) + " : " + reason_str);
}

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
#define LOCATION __FILE__  ":" STRINGIFY(__LINE__)

struct SSLInitializer {
    SSLInitializer() {
        SSL_library_init();
        SSLeay_add_ssl_algorithms();
        SSL_load_error_strings();
    }
};

SSL_CTX * prepareSSLContext(const clickhouse::SSLParams & context_params) {
    static const SSLInitializer ssl_initializer;

    const SSL_METHOD *method = TLS_client_method();
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> ctx(SSL_CTX_new(method), &SSL_CTX_free);

    if (!ctx)
        throw std::runtime_error("Failed to initialize SSL context");

#define HANDLE_SSL_CTX_ERROR(statement) do { \
    if (const auto ret_code = statement; !ret_code) \
        throwSSLError(nullptr, ERR_peek_error(), LOCATION, STRINGIFY(statement)); \
} while(false);

    if (context_params.use_default_ca_locations)
        HANDLE_SSL_CTX_ERROR(SSL_CTX_set_default_verify_paths(ctx.get()));
    if (!context_params.path_to_ca_directory.empty())
        HANDLE_SSL_CTX_ERROR(
            SSL_CTX_load_verify_locations(
                ctx.get(),
                nullptr,
                context_params.path_to_ca_directory.c_str())
        );

    for (const auto & f : context_params.path_to_ca_files)
        HANDLE_SSL_CTX_ERROR(SSL_CTX_load_verify_locations(ctx.get(), f.c_str(), nullptr));

    if (context_params.context_options != -1)
        SSL_CTX_set_options(ctx.get(), context_params.context_options);
    if (context_params.min_protocol_version != -1)
        HANDLE_SSL_CTX_ERROR(
            SSL_CTX_set_min_proto_version(ctx.get(), context_params.min_protocol_version));
    if (context_params.max_protocol_version != -1)
        HANDLE_SSL_CTX_ERROR(
            SSL_CTX_set_max_proto_version(ctx.get(), context_params.max_protocol_version));

    return ctx.release();
}



}

#define HANDLE_SSL_ERROR(statement) do { \
    if (const auto ret_code = statement; ret_code <= 0) \
        throwSSLError(ssl_, SSL_get_error(ssl_, ret_code), LOCATION, STRINGIFY(statement)); \
} while(false);

namespace clickhouse {

SSLContext::SSLContext(SSL_CTX & context)
    : context_(&context)
{
    SSL_CTX_up_ref(context_);
}

SSLContext::SSLContext(const SSLParams & context_params)
    : context_(prepareSSLContext(context_params))
{
}

SSLContext::~SSLContext() {
    SSL_CTX_free(context_);
}

SSL_CTX * SSLContext::getContext() {
    return context_;
}

/* // debug macro for tracing SSL state
#define LOG_SSL_STATE() std::cerr << "!!!!" << LOCATION << " @" << __FUNCTION__ \
    << "\t" << SSL_get_version(ssl_) << " state: "  << SSL_state_string_long(ssl_) \
    << "\n\t handshake state: " << SSL_get_state(ssl_) \
    << std::endl
*/
SSLSocket::SSLSocket(const NetworkAddress& addr, const SSLParams & ssl_params, SSLContext& context)
    : Socket(addr),
    ssl_(SSL_new(context.getContext()))
{
    if (!ssl_)
        throw std::runtime_error("Failed to create SSL instance");

    HANDLE_SSL_ERROR(SSL_set_fd(ssl_, handle_));
    if (ssl_params.use_SNI)
        HANDLE_SSL_ERROR(SSL_set_tlsext_host_name(ssl_, addr.Host().c_str()));

    SSL_set_connect_state(ssl_);
    HANDLE_SSL_ERROR(SSL_connect(ssl_));
    HANDLE_SSL_ERROR(SSL_set_mode(ssl_, SSL_MODE_AUTO_RETRY));

    if(const auto verify_result = SSL_get_verify_result(ssl_); verify_result != X509_V_OK) {
        auto error_message = X509_verify_cert_error_string(verify_result);
        auto ssl_session = SSL_get_session(ssl_);
        auto cert = SSL_SESSION_get0_peer(ssl_session);

        throw std::runtime_error("Failed to verify SSL connection, X509_v error: "
                                 + std::to_string(verify_result)
                                 + " " + error_message + "\n" + getCertificateInfo(cert));
    }

    if (ssl_params.use_SNI) {
        auto ssl_session = SSL_get_session(ssl_);
        auto peer_cert = SSL_SESSION_get0_peer(ssl_session);
        auto hostname = addr.Host();
        char * out_name = nullptr;

        std::unique_ptr<ASN1_OCTET_STRING, decltype(&ASN1_OCTET_STRING_free)> addr(a2i_IPADDRESS(hostname.c_str()), &ASN1_OCTET_STRING_free);
        if (addr) {
            // if hostname is actually an IP address
            HANDLE_SSL_ERROR(X509_check_ip(
                    peer_cert,
                    ASN1_STRING_get0_data(addr.get()),
                    ASN1_STRING_length(addr.get()),
                    0));
        } else {
            HANDLE_SSL_ERROR(X509_check_host(peer_cert, hostname.c_str(), hostname.length(), 0, &out_name));
        }
    }
}

SSLSocket::~SSLSocket() {
    SSL_free(ssl_);
}

std::unique_ptr<InputStream> SSLSocket::makeInputStream() const {
    return std::make_unique<SSLSocketInput>(ssl_);
}

std::unique_ptr<OutputStream> SSLSocket::makeOutputStream() const {
    return std::make_unique<SSLSocketOutput>(ssl_);
}

SSLSocketInput::SSLSocketInput(SSL *ssl)
    : ssl_(ssl)
{}

SSLSocketInput::~SSLSocketInput() = default;

size_t SSLSocketInput::DoRead(void* buf, size_t len) {
    size_t actually_read;
    HANDLE_SSL_ERROR(SSL_read_ex(ssl_, buf, len, &actually_read));
    return actually_read;
}

SSLSocketOutput::SSLSocketOutput(SSL *ssl)
    : ssl_(ssl)
{}

SSLSocketOutput::~SSLSocketOutput() = default;

void SSLSocketOutput::DoWrite(const void* data, size_t len) {
    HANDLE_SSL_ERROR(SSL_write(ssl_, data, len));
}

}
