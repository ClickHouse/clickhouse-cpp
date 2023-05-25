#include "endpoints_iterator.h"
#include "../client.h"

namespace clickhouse {

RoundRobinEndpointsIterator::RoundRobinEndpointsIterator(const ClientOptions& opts) :
     hosts (opts.hosts) 
   , ports (opts.ports)
   , current_index (0) 
   , iteration_counter(0)
{

}

std::string RoundRobinEndpointsIterator::getHostAddr() const
{
   return hosts[current_index];
}

unsigned int RoundRobinEndpointsIterator::getPort() const
{
   return ports[current_index];
}

void RoundRobinEndpointsIterator::ResetIterations()
{
   iteration_counter = 0;
}

void RoundRobinEndpointsIterator::next()
{
   current_index = (current_index + 1) % hosts.size();
   iteration_counter++;
}

bool RoundRobinEndpointsIterator::nextIsExist() const
{
   return iteration_counter + 1 < hosts.size();
}

RoundRobinEndpointsIterator::~RoundRobinEndpointsIterator() = default;

}
