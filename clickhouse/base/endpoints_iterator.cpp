#include "endpoints_iterator.h"
#include <clickhouse/client.h>

namespace clickhouse {

namespace {

const std::vector<Endpoint> & ValidateEndpoints(const std::vector<Endpoint>& endpoints)
{
    if (endpoints.empty()) {
        throw ValidationError("The list of endpoints is empty");
    }
    return endpoints;
}

} // anonymous namespace

RoundRobinEndpointsIterator::RoundRobinEndpointsIterator(const std::vector<Endpoint>& _endpoints)
   :  endpoints (ValidateEndpoints(_endpoints))
   // set `current_index` to the value such that `Next` returns an element at index 0
   , current_index (endpoints.size() - 1ull)
{
}

Endpoint RoundRobinEndpointsIterator::Next()
{
   current_index = (current_index + 1ull) % endpoints.size();
   return endpoints[current_index];
}

RoundRobinEndpointsIterator::~RoundRobinEndpointsIterator() = default;

}
