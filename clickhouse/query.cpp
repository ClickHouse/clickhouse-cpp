#include "query.h"

namespace clickhouse {

Query::Query()
{ }

Query::Query(const char* query)
    : query_(query)
{
}

Query::Query(const std::string& query)
    : query_(query)
{
}

Query::~Query()
{ }

}
