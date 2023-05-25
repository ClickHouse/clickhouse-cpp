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

   virtual void Next() = 0;
   // Get the address of current endpoint. 
   virtual std::string GetHostAddr() const = 0;

   // Get the port of current endpoint.
   virtual unsigned int GetPort() const = 0;

   // Reset iterations.
   virtual void ResetIterations() = 0; 
   virtual bool NextIsExist() const = 0;
};

class RoundRobinEndpointsIterator : public EndpointsIteratorBase
{
 public:
   RoundRobinEndpointsIterator(const ClientOptions& opts);
   std::string GetHostAddr() const override;
   unsigned int GetPort() const override;
   void ResetIterations() override;
   bool NextIsExist() const override;
   void Next() override;
    
   ~RoundRobinEndpointsIterator() override;

 private:

   const std::vector<std::string>& hosts;
   const std::vector<unsigned int>& ports;
   int current_index;
   size_t iteration_counter;
};

}
