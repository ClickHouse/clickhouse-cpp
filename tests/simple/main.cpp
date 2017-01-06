#include <clickhouse/client.h>

#include <iostream>

using namespace clickhouse;
using namespace std;

int main() {
    Client client("localhost");

    try {
        client.Connect();
        std::cout << "connected" << std::endl;

        client.SendQuery("SELECT * FROM system.numbers LIMIT 10");
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
