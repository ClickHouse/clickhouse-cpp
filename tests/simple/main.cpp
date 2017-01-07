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

    void OnProgress(const Progress& progress) override {
        std::cerr << "rows : " << progress.rows << std::endl;
        std::cerr << "bytes : " << progress.bytes << std::endl;
        std::cerr << "total_rows : " << progress.total_rows << std::endl;
    }
};

static const std::string query =
    //"SELECT * FROM system.numbers LIMIT 10";
    "SELECT number, toString(number) || 'x' as string FROM system.numbers LIMIT 10";

int main() {
    Client client("localhost");

    try {
        EventHandler h;

        client.Connect();
        std::cout << "connected" << std::endl;

        client.ExecuteQuery(query, &h);
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
