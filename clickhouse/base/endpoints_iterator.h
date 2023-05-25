#pragma once

#include "../client.h"
#include <vector>

namespace clickhouse {

struct ClientOptions;

/**
 * Base class for iterating through endpoints.
*/
class EndpointsIteratorBase
{
 public: 
   virtual ~EndpointsIteratorBase() = default;

   virtual void next() = 0;
   // Get the address of current endpoint. 
   virtual const std::string getHostAddr() const = 0;

   // Get the port of current endpoint.
   virtual unsigned int getPort() const = 0;

   // Reset iterations.
   virtual void ResetIterations() = 0; 
   virtual bool nextIsExist() const = 0;
};

/**
  * Client tries to connect to those endpoints one by one, on the round-robin basis:
  * first default enpoint, then each of endpoints, from begin() to end(), 
  * if previous are inaccessible.
  */
class RoundRobinEndpointsIterator : public EndpointsIteratorBase
{
 public:
    RoundRobinEndpointsIterator(const ClientOptions& opts);
    const std::string getHostAddr() const override;
    unsigned int getPort() const override;
    void ResetIterations() override;
    bool nextIsExist() const override;
    void next() override;
    
    ~RoundRobinEndpointsIterator() override;

 private:

   const std::vector<std::string>& hosts;
   const std::vector<unsigned int>& ports;
   int current_index;
   bool reseted;
   size_t iteration_counter;
};

}
