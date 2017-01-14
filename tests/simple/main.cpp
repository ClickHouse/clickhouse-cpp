#include <clickhouse/client.h>
#include <clickhouse/type_parser.h>

#include <iostream>

using namespace clickhouse;
using namespace std;

class EventHandler : public QueryEvents {
public:
    void OnData(const Block& block) override {
        for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
            std::cerr << bi.Name() << " ";
        }
        std::cerr << std::endl;

        for (size_t i = 0; i < block.Rows(); ++i) {
            for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
                bi.Column()->Print(std::cerr, i);
                std::cerr << " ";
            }
            std::cerr << std::endl;
        }
    }

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

    void OnFinish() override {
        std::cerr << "finish" << std::endl;
    }
};

static const std::string query =
    //"SELECT * FROM system.numbers LIMIT 10";
    "SELECT number, number / 3.0, toString(number) || 'x' as string FROM system.numbers LIMIT 10";
    //"SELECT type, user, read_rows, address FROM system.query_log LIMIT 10";
    //"SELECT user, count(*) FROM system.query_log GROUP BY user";
    //"SELECT 1000, '1', (1, 1, (1, 'weew'))";
    //"SELECT 1, '1', (1, 1, (1, 'weew', [1, 1], NULL))";
    //"SELECT now()";

inline void PrintAst(const TypeAst& ast, int level = 0) {
    std::cout << std::endl;
    for (int i = 0; i < level * 2; ++i) {
        std::cout << " ";
    }
    std::cout << std::string(ast.name);
    if (ast.size) {
        std::cout << "[" << ast.size << "]";
    }
    for (const auto& elem : ast.elements) {
        PrintAst(elem, level + 1);
    }
}

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
