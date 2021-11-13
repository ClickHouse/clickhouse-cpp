#pragma once

#include "socket.h"

#include <memory>

typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

namespace clickhouse {

struct SSLParams
{
    std::vector<std::string> path_to_ca_files;
    std::string path_to_ca_directory;
    bool use_default_ca_locations;
    int context_options;
    int min_protocol_version;
    int max_protocol_version;
    bool use_SNI;
};

class SSLContext
{
public:
    explicit SSLContext(SSL_CTX & context);
    explicit SSLContext(const SSLParams & context_params);
    ~SSLContext() = default;

    SSLContext(const SSLContext &) = delete;
    SSLContext& operator=(const SSLContext &) = delete;
    SSLContext(SSLContext &&) = delete;
    SSLContext& operator=(SSLContext &) = delete;

private:
    friend class SSLSocket;
    SSL_CTX * getContext();

private:
    std::unique_ptr<SSL_CTX, void (*)(SSL_CTX*)> context_;
};

class SSLSocket : public Socket {
public:
    explicit SSLSocket(const NetworkAddress& addr, const SSLParams & ssl_params, SSLContext& context);
    SSLSocket(SSLSocket &&) = default;
    ~SSLSocket() = default;

    SSLSocket(const SSLSocket & ) = delete;
    SSLSocket& operator=(const SSLSocket & ) = delete;

    std::unique_ptr<InputStream> makeInputStream() const override;
    std::unique_ptr<OutputStream> makeOutputStream() const override;

private:
    std::unique_ptr<SSL, void (*)(SSL *s)> ssl_;
};

class SSLSocketInput : public InputStream {
public:
    explicit SSLSocketInput(SSL *ssl);
    ~SSLSocketInput() = default;

    bool Skip(size_t /*bytes*/) override {
        return false;
    }

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    // Not owning
    SSL *ssl_;
};

class SSLSocketOutput : public OutputStream {
public:
    explicit SSLSocketOutput(SSL *ssl);
    ~SSLSocketOutput() = default;

protected:
    size_t DoWrite(const void* data, size_t len) override;

private:
    // Not owning
    SSL *ssl_;
};

}
