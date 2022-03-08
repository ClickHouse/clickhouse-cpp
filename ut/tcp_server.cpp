#include "tcp_server.h"

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__WIN32__) || defined(_WIN32) || defined(_WIN64)
#   include <winsock2.h>
#else
#   include <netinet/in.h>
#   include <sys/socket.h>
#   include <unistd.h>
#endif

#include <thread>

namespace clickhouse {

LocalTcpServer::LocalTcpServer(int port)
    : port_(port)
    , serverSd_(-1)
{}

LocalTcpServer::~LocalTcpServer() {
    stop();
}

void LocalTcpServer::start() {
    //setup a socket
    sockaddr_in servAddr;
    memset((char*)&servAddr, 0, sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    servAddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    servAddr.sin_port = htons(port_);
    serverSd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSd_ < 0) {
        std::cerr << "Error establishing server socket" << std::endl;
        throw std::runtime_error("Error establishing server socket");
    }
    int enable = 1;

#if defined(__WIN32__) || defined(_WIN32) || defined(_WIN64)
    auto res = setsockopt(serverSd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&enable, sizeof(enable));
#else
    auto res = setsockopt(serverSd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
#endif

    if (res < 0) {
        std::cerr << "setsockopt(SO_REUSEADDR) failed" << std::endl;
    }
    //bind the socket to its local address
    int bindStatus = bind(serverSd_, (struct sockaddr*) &servAddr, sizeof(servAddr));
    if (bindStatus < 0) {
        auto err = errno;
        const char * error = strerror(err);

        std::cerr << "Error binding socket to local address: " << error << std::endl;
        throw std::runtime_error("Error binding socket to local address: " + std::string(error ? error : ""));
    }
    listen(serverSd_, 3);
}

void LocalTcpServer::stop() {
    if(serverSd_ > 0) {

#if defined(__WIN32__) || defined(_WIN32) || defined(_WIN64)
        shutdown(serverSd_, SD_BOTH);
        closesocket(serverSd_);
#else
        shutdown(serverSd_, SHUT_RDWR);
        close(serverSd_);
#endif
        serverSd_ = -1;
    }
}

}
