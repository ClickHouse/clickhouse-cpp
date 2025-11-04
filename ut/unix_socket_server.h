#pragma once

#include <string>
#include "clickhouse/base/platform.h"

#if defined(_unix_)

namespace clickhouse {

class LocalUnixSocketServer {
public:
    LocalUnixSocketServer(const std::string& socket_path);
    ~LocalUnixSocketServer();

    void start();
    void stop();

private:
    std::string socket_path_;
    int serverSd_;
};

}

#endif // defined(_unix_)
