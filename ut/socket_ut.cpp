#include "tcp_server.h"

#include <clickhouse/base/socket.h>
#include <contrib/gtest/gtest.h>

#include <iostream>
#include <stdio.h>
#include <string.h>
#include <thread>

using namespace clickhouse;

TEST(Socketcase, connecterror) {
   int port = 9978;
   NetworkAddress addr("localhost", std::to_string(port));
   LocalTcpServer server(port);
   server.start();
   std::this_thread::sleep_for(std::chrono::seconds(1));
   try {
      SocketConnect(addr);
   } catch (const std::system_error& e) {
      FAIL();
   }
   std::this_thread::sleep_for(std::chrono::seconds(1));
   server.stop();
   try {
      SocketConnect(addr);
      FAIL();
   } catch (const std::system_error& e) {
      ASSERT_NE(EINPROGRESS,e.code().value());
   }
}
