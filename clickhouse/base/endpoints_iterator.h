#pragma once

#include "clickhouse/client.h"
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
   virtual uint16_t GetPort() const = 0;

   // Reset iterations.
   virtual void ResetIterations() = 0; 
   virtual bool NextIsExist() const = 0;
};

class RoundRobinEndpointsIterator : public EndpointsIteratorBase
{
 public:
    explicit RoundRobinEndpointsIterator(const std::vector<Endpoint>& opts);
    std::string GetHostAddr() const override;
    uint16_t GetPort() const override;
    void ResetIterations() override;
    bool NextIsExist() const override;
    void Next() override;
    
    ~RoundRobinEndpointsIterator() override;

 private:
    const std::vector<Endpoint>& endpoints;
    int current_index;
    size_t iteration_counter;
};

}
