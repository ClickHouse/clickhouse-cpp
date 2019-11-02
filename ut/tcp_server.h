#pragma once

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
    int serverSd_;
};

}
