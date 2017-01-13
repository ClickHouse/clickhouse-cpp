#include <clickhouse/client.h>

#include <iostream>

using namespace clickhouse;
using namespace std;

class EventHandler : public QueryEvents {
public:
    void OnData() override
    { }

    void OnServerException(const Exception& e) override {
        std::cerr << e.code << std::endl;
        std::cerr << e.name << std::endl;
        std::cerr << e.display_text << std::endl;
        std::cerr << e.stack_trace << std::endl;
        std::cerr << int(!!e.nested) << std::endl;
    }

    void OnProfile(const Profile& profile) override {
        std::cerr << "rows : " << profile.rows << std::endl;
        std::cerr << "blocks : " << profile.blocks << std::endl;
        std::cerr << "bytes : " << profile.bytes << std::endl;
        std::cerr << "rows_before_limit : "
                  << profile.rows_before_limit << std::endl;
        std::cerr << "applied_limit : "
                  << int(profile.applied_limit) << std::endl;
        std::cerr << "calculated_rows_before_limit : "
                  << int(profile.calculated_rows_before_limit) << std::endl;
    }

    void OnProgress(const Progress& progress) override {
        std::cerr << "rows : " << progress.rows << std::endl;
        std::cerr << "bytes : " << progress.bytes << std::endl;
        std::cerr << "total_rows : " << progress.total_rows << std::endl;
    }
};

static const std::string query =
    //"SELECT * FROM system.numbers LIMIT 10";
    //"SELECT number, number / 3.0, toString(number) || 'x' as string FROM system.numbers LIMIT 10";
    //"SELECT type, user, read_rows, address FROM system.query_log LIMIT 10";
    "SELECT user, count(*) FROM system.query_log GROUP BY user";

int main() {
    Client client("localhost");

    try {
        EventHandler h;

        client.ExecuteQuery(query, &h);
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
