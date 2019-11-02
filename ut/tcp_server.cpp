#include "tcp_server.h"

#include <iostream>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

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
    bzero((char*)&servAddr, sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servAddr.sin_port = htons(port_);
    serverSd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSd_ < 0) {
        std::cerr << "Error establishing server socket" << std::endl;
        throw std::runtime_error("Error establishing server socket");
    }
    int enable = 1;
    if (setsockopt(serverSd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        std::cerr << "setsockopt(SO_REUSEADDR) failed" << std::endl;
    }
    //bind the socket to its local address
    int bindStatus = bind(serverSd_, (struct sockaddr*) &servAddr, sizeof(servAddr));
    if (bindStatus < 0) {
        std::cerr << "Error binding socket to local address" << std::endl;
        throw std::runtime_error("Error binding socket to local address");
    }
    listen(serverSd_, 3);
}

void LocalTcpServer::stop() {
    if(serverSd_ > 0) {
        shutdown(serverSd_, SHUT_RDWR);
        close(serverSd_);
        serverSd_ = -1;
    }
}

}
