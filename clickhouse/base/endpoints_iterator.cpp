#include "endpoints_iterator.h"
#include <clickhouse/client.h>

namespace clickhouse {

RoundRobinEndpointsIterator::RoundRobinEndpointsIterator(const std::vector<Endpoint>& _endpoints) :
     endpoints (_endpoints)
   , current_index (0) 
   , iteration_counter(0)
{

}

std::string RoundRobinEndpointsIterator::GetHostAddr() const
{
   return endpoints[current_index].host;
}

unsigned int RoundRobinEndpointsIterator::GetPort() const
{
   return endpoints[current_index].port;
}

void RoundRobinEndpointsIterator::ResetIterations()
{
   iteration_counter = 0;
}

void RoundRobinEndpointsIterator::Next()
{
   current_index = (current_index + 1) % endpoints.size();
   iteration_counter++;
}

bool RoundRobinEndpointsIterator::NextIsExist() const
{
   return iteration_counter + 1 < endpoints.size();
}

RoundRobinEndpointsIterator::~RoundRobinEndpointsIterator() = default;

}
