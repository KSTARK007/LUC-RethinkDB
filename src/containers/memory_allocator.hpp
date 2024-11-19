#pragma once
#include <cstdlib>
#include <iostream>
#include <cstring>
#include <mutex>
#include <cstddef>  // for size_t
#include <unistd.h> // for sysconf
#include <containers/page_metadata.hpp>
#include <iomanip> // for std::hex, std::setw, std::setfill
#include "containers/rdma.hpp"
#include <random>

#define MAX_POOL_SIZE (uint64_t)(20) * 1024 * 1024 * 1024 // 10 GB
#define SERVER_PORT_MAIN_CACHE 5000

typedef uint64_t block_id_t;

class MemoryPool
{
public:
    // Constructor to initialize the memory pool with memaligned pages
    MemoryPool(size_t pool_size, size_t alignment = sysconf(_SC_PAGESIZE)); // Use system page size as default alignment

    ~MemoryPool();

    void *allocate(size_t size);

    void deallocate(void *ptr);

    uint64_t get_offset(void *ptr);

    char *read_block(uint64_t offset, size_t size);

    void print_block_content(void *ptr, size_t size);

    void populate_block();

    std::pair<RDMAClient *, size_t> check_block_exists(block_id_t block_id);

    void *get_buffer_from_offset(RDMAClient *client, uint64_t offset, size_t size);

    void print_allocation_memory();

    bool is_within_cache_limit(block_id_t block_id);
    struct FreeBlock
    {
        FreeBlock *next;
    };

    char *memory;          // Start of the memory pool
    char *mem_start;       // Start of the memory pool
    char *pool_end;        // End of the memory pool
    FreeBlock *free_list;  // Linked list of free blocks
    std::mutex pool_mutex; // Mutex for thread-safe operations
    RDMAServer rdma_connection;
    std::vector<RDMAClient *> RemoteMemoryPool;
    std::vector<RDMAClient *> RemoteMetadata;
    int max_block_cap;
    int min_block_cap;

    std::atomic<bool> server_ready;

    ConfigParser *configs;
};

class PageAllocator
{
public:
    static void *allocate(size_t size)
    {
        return memory_pool->allocate(size); // Allocate from the global memory pool
    }

    static void deallocate(void *ptr)
    {
        memory_pool->deallocate(ptr); // Deallocate to the global memory pool
    }

    static void initialize_pool(size_t pool_size)
    {
        if (!memory_pool)
        {
            memory_pool = new MemoryPool(pool_size);
        }
    }

    static void destroy_pool()
    {
        std::cout << "Destroying memory pool..." << std::endl;
        delete memory_pool;
        memory_pool = nullptr;
    }

    uint64_t get_offset(void *ptr)
    {
        return memory_pool->get_offset(ptr);
    }

    static MemoryPool *memory_pool;
};
