#include <clickhouse/client.h>

#include <iostream>

using namespace clickhouse;
using namespace std;

class EventHandler : public QueryEvents {
public:
    void OnData() override
    { }

    void OnException() override
    { }

    void OnProgress() override
    { }
};

int main() {
    Client client("localhost");

    try {
        EventHandler h;

        client.Connect();
        std::cout << "connected" << std::endl;

        client.ExecuteQuery("SELECT * FROM system.numbers LIMIT 10", &h);
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
