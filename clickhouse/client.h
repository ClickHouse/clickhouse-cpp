#pragma once

#include <memory>
#include <string>

namespace clickhouse {

/**
 *
 */
class Client {
public:
    Client();
    explicit Client(const std::string& host, int port = 9000);
    ~Client();

    void Connect();

    void SendQuery(const std::string& query);

private:
    const std::string host_;
    const int port_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
