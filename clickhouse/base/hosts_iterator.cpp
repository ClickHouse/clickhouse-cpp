#include "hosts_iterator.h"
#include "../client.h"

namespace clickhouse {

RoundRobinHostsIterator::RoundRobinHostsIterator(const ClientOptions& opts) :
     hosts (opts.hosts) 
   , ports (opts.ports)
   , current_index (0) 
   , reseted (true)
   , iteration_counter(0)
{
   
}

const std::string RoundRobinHostsIterator::getHostAddr() const
{
   return hosts[current_index];
}

unsigned int RoundRobinHostsIterator::getPort() const
{
   return ports[current_index];
}

void RoundRobinHostsIterator::ResetIterations()
{
   reseted = true;
   iteration_counter = 0;
}

void RoundRobinHostsIterator::next()
{
   current_index = (current_index + 1) % hosts.size();
   iteration_counter++;
}

bool RoundRobinHostsIterator::nextIsExist() const
{
   return iteration_counter < hosts.size();
}

RoundRobinHostsIterator::~RoundRobinHostsIterator() = default;


}