#pragma once

#include "../client.h"
#include <vector>

namespace clickhouse {

struct ClientOptions;

class HostsIteratorBase
{
 public: 
   virtual ~HostsIteratorBase() = default;

   virtual void next() = 0;
   virtual const std::string getHostAddr() const = 0;
   virtual unsigned int getPort() const = 0;
   virtual void ResetIterations() = 0; 
   virtual bool nextIsExist() const = 0;
};


class RoundRobinHostsIterator : public HostsIteratorBase
{
 public:
    RoundRobinHostsIterator(const ClientOptions& opts);
    const std::string getHostAddr() const override;
    unsigned int getPort() const override;
    void ResetIterations() override;
    bool nextIsExist() const override;
    void next() override;
    
    ~RoundRobinHostsIterator() override;

 private:

   const std::vector<std::string>& hosts;
   const std::vector<unsigned int>& ports;
   int current_index;
   bool reseted;
   size_t iteration_counter;
};

}