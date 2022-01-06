#pragma once

#include <winsock2.h>

namespace clickhouse {

class LocalTcpServer {
public:
    LocalTcpServer(int port);
    ~LocalTcpServer();

    void start();
    void stop();

private:
    void startImpl();

private:
    int port_;
    SOCKET serverSd_;
};

}
