#include <clickhouse/client.h>
#include <clickhouse/type_parser.h>

#include <iostream>

using namespace clickhouse;
using namespace std;

inline void PrintBlock(const Block& block) {
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

inline Block CreateBlock() {
    Block b;

    ColumnUInt64 c64;
    c64.Append(1);
    c64.Append(17);

    ColumnString cS;
    cS.Append("one");
    cS.Append("seventeen");

    b.AppendColumn("id"  , "UInt64", ColumnRef(new ColumnUInt64(c64)));
    b.AppendColumn("name", "String", ColumnRef(new ColumnString(cS)));

    return b;
}

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
    ClientOptions opts;
    opts.host = "localhost";
    Client client(opts);

    try {
        client.Execute("CREATE TABLE IF NOT EXISTS test.client (id UInt64, name String) ENGINE = Memory");
        client.Select("SELECT id, name FROM test.client", [](const Block& block)
            {
                PrintBlock(block);
            }
        );

        client.Insert("test.client", CreateBlock());

        client.Select("SELECT id, name FROM test.client", [](const Block& block)
            {
                PrintBlock(block);
            }
        );
        client.Execute("DROP TABLE test.client");
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }

    return 0;
}
