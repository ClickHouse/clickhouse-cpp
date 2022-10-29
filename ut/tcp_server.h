#pragma once

#include <memory>

namespace clickhouse {

class InputStream;
class OutputStream;

class LocalTcpServer {
public:
    LocalTcpServer(int port);
    ~LocalTcpServer();

    void start();
    void stop();

private:

    int port_;
#if defined(_WIN32)
    size_t serverSd_; 
#else
    int serverSd_;
#endif
};

}
