#include "unix_socket_server.h"
#include "clickhouse/base/platform.h"

#if defined(_unix_)

#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace clickhouse {

LocalUnixSocketServer::LocalUnixSocketServer(const std::string& socket_path)
    : socket_path_(socket_path)
    , serverSd_(-1)
{}

LocalUnixSocketServer::~LocalUnixSocketServer() {
    stop();
}

void LocalUnixSocketServer::start() {
    struct sockaddr_un servAddr;
    memset(&servAddr, 0, sizeof(servAddr));
    servAddr.sun_family = AF_UNIX;
    
    if (socket_path_.size() >= sizeof(servAddr.sun_path)) {
        throw std::runtime_error("UNIX socket path too long");
    }
    
    // Remove existing socket file if it exists
    unlink(socket_path_.c_str());
    
    strncpy(servAddr.sun_path, socket_path_.c_str(), sizeof(servAddr.sun_path) - 1);
    
    serverSd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (serverSd_ < 0) {
        std::cerr << "Error establishing UNIX socket" << std::endl;
        throw std::runtime_error("Error establishing UNIX socket");
    }
    
    int bindStatus = bind(serverSd_, (struct sockaddr*) &servAddr, sizeof(servAddr));
    if (bindStatus < 0) {
        auto err = errno;
        const char * error = strerror(err);
        
        std::cerr << "Error binding UNIX socket: " << error << std::endl;
        close(serverSd_);
        serverSd_ = -1;
        throw std::runtime_error("Error binding UNIX socket: " + std::string(error ? error : ""));
    }
    
    listen(serverSd_, 3);
}

void LocalUnixSocketServer::stop() {
    if (serverSd_ >= 0) {
        shutdown(serverSd_, SHUT_RDWR);
        close(serverSd_);
        serverSd_ = -1;
    }
    
    // Remove socket file
    unlink(socket_path_.c_str());
}

}

#endif // defined(_unix_)
