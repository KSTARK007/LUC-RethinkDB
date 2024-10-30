// rdma.hpp
#pragma once

#include <string>
#include <mutex>
#include <iostream>
#include <optional>
#include <infinity/infinity.h>
#include <random>

class RDMAConnection
{
public:
    // Constructor
    RDMAConnection(const std::string &ip, uint64_t index, bool isLocal = false);

    // Destructor
    ~RDMAConnection();

    // Initialize RDMA
    void init(void *memory_region, uint64_t size, int server_port);

    // Static function to find NIC containing a specific string
    static std::string findNICContaining(const std::string &s);

    // Accessors
    infinity::core::Context *getContext() const { return context; }
    infinity::queues::QueuePair *getQueuePair() const { return qp; }
    infinity::memory::RegionToken *getRemoteBufferToken() const { return remote_buffer_token; }
    infinity::memory::Buffer *getBuffer() const { return buffer; }
    bool isLocalNode() const { return isLocal; }

private:
    // Member variables
    std::string ip;
    uint64_t index;
    infinity::core::Context *context;
    infinity::queues::QueuePair *qp;
    infinity::queues::QueuePairFactory *qp_factory;
    infinity::memory::RegionToken *remote_buffer_token;
    infinity::memory::Buffer *buffer;
    bool isLocal;
    std::mutex rdma_mutex; // For thread safety if needed
};
