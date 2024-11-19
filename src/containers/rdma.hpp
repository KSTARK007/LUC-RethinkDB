// rdma.hpp
#pragma once

#include <string>
#include <mutex>
#include <iostream>
#include <optional>
#include <infinity/infinity.h>
#include <random>

#define CLIENT_BUFFER_SIZE (64 * 1024) // 64 KB buffer size
#define MAX_METADATA_BLOCKS 100000
#define ACTUAL_DATA_BLOCKS 8000

#define RDMA_TO_LOCAL_FREQUENCY 100

class PageMap;
class RDMAServer
{
public:
    // Constructor
    RDMAServer(const std::string &ip, uint64_t index, bool isLocal = false);

    // Destructor
    ~RDMAServer();

    // Initialize RDMA
    void init(void *memory_region, uint64_t size, int server_port, int expected_connections);

    // Static function to find NIC containing a specific string
    static std::string findNICContaining(const std::string &s);

    // Accessors
    infinity::core::Context *getContext() const { return context; }
    infinity::queues::QueuePair *getQueuePair() const { return qp; }
    infinity::memory::RegionToken *getRemoteBufferToken() const { return remote_buffer_token; }
    infinity::memory::Buffer *getBuffer() const { return buffer; }
    bool isLocalNode() const { return isLocal; }
    std::string getIP() const { return ip; }

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

    std::vector<infinity::queues::QueuePair *> qp_list;
};

class RDMAClient
{
public:
    RDMAClient(const std::string &ip, uint16_t port, bool isMetaData);
    ~RDMAClient();

    // Method to connect to the server
    bool connectToServer();

    // Simplified method to perform RDMA read with a fixed 64KB buffer
    void performRDMARead(uint64_t total_buffer_size);

    void readMetadata();

    void print_client()
    {
        std::cout << "Client IP: " << ip << ", Port: " << port << ", Device hint: " << device_hint << ", Output file: " << output_file << std::endl;
        std::cout << "Is metadata: " << isMetaData << std::endl;
        std::cout << "Context: " << context << ", Queue pair: " << qp << ", Queue pair factory: " << qp_factory << ", Remote buffer token: " << remote_buffer_token << std::endl;
        std::cout << "Page buffer: " << page_buffer << ", Request token: " << request_token << std::endl;
        std::cout << "meta_data_buffer: " << meta_data_buffer << std::endl;
    }

    PageMap *getPageMap() const { return page_map; }
    void **getMetaDataTmpBuffer() { return &meta_data_tmp_buffer; }
    void setMetaDataBuffer(void *buffer) { meta_data_tmp_buffer = buffer; }
    void *getMetaDataBuffer() { return meta_data_buffer->getData(); }
    void setPageMap(PageMap *map) { page_map = map; }
    void updateMetaDataBuffer()
    {
        memcpy(meta_data_tmp_buffer, meta_data_buffer->getData(), meta_data_buffer->getSizeInBytes());
    }

    void *getPageFromOffset(uint64_t offset, size_t size);
    std::string getIP() const { return ip; }

    // Method to perform frequency map lookup and addition
    bool performFrequencyMapLookup(uint64_t block_id);

    void addFrequencyMapEntry(uint64_t block_id)
    {
        if (frequency_map.find(block_id) == frequency_map.end())
        {
            frequency_map[block_id] = 1;
        }
        else
        {
            frequency_map[block_id]++;
        }
    }

private:
    std::string ip;
    uint16_t port;
    std::string device_hint;
    std::string output_file;
    bool isMetaData;

    // RDMA resources
    infinity::core::Context *context;
    infinity::queues::QueuePair *qp;
    infinity::queues::QueuePairFactory *qp_factory;
    infinity::memory::RegionToken *remote_buffer_token;

    // Pre-allocated buffer and request token for RDMA read
    infinity::memory::Buffer *page_buffer;
    infinity::memory::Buffer *meta_data_buffer;
    infinity::requests::RequestToken *request_token;

    PageMap *page_map;
    void *meta_data_tmp_buffer;
    void *page_buffer_tmp;
    std::mutex page_map_mutex;
    std::unordered_map<uint64_t, uint64_t> frequency_map;
};