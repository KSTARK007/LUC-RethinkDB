#pragma once

#include <cstdlib>
#include <iostream>
#include <cstring>
#include <mutex>
#include <cstddef>  // for size_t
#include <unistd.h> // for sysconf
#include <containers/page_metadata.hpp>

#define MAX_POOL_SIZE (uint64_t)(10) * 1024 * 1024 * 1024 // 10 GB

typedef uint64_t block_id_t;

class MemoryPool
{
public:
    // Constructor to initialize the memory pool with memaligned pages
    MemoryPool(size_t pool_size, size_t alignment = sysconf(_SC_PAGESIZE)) // Use system page size as default alignment
    {
        int result = posix_memalign(reinterpret_cast<void **>(&memory), alignment, pool_size);
        if (result != 0)
        {
            std::cerr << "Failed to allocate aligned memory pool with size: " << pool_size << std::endl;
            memory = nullptr;
            pool_end = nullptr;
        }
        else
        {
            pool_end = memory + pool_size;
            free_list = nullptr;
            std::cout << "Aligned memory pool created with size: " << pool_size << " and alignment: " << alignment << std::endl;
        }
    }

    ~MemoryPool()
    {
        free(memory);
        std::cout << "Memory pool destroyed." << std::endl;
    }

    // Allocate memory from the aligned pool
    void *allocate(size_t size) // Default alignment to page size
    {
        std::lock_guard<std::mutex> lock(pool_mutex); // Thread-safe access

        // Adjust allocation to ensure alignment
        size_t alignment = sysconf(_SC_PAGESIZE);
        char *aligned_memory = reinterpret_cast<char *>((reinterpret_cast<uintptr_t>(memory) + (alignment - 1)) & ~(alignment - 1));

        if (aligned_memory + size > pool_end)
        {
            std::cerr << "Out of memory in pool" << std::endl;
            return nullptr;
        }

        memory = aligned_memory + size; // Move the pointer forward
        std::cout << "Allocated " << size << " bytes from aligned memory pool at address " << static_cast<void *>(aligned_memory) << std::endl;
        return aligned_memory;
    }

    // Deallocate function stub (you can implement this later if needed)
    void deallocate(void *ptr)
    {
        return;
    }

private:
    struct FreeBlock
    {
        FreeBlock *next;
    };

    char *memory;          // Start of the memory pool
    char *pool_end;        // End of the memory pool
    FreeBlock *free_list;  // Linked list of free blocks
    std::mutex pool_mutex; // Mutex for thread-safe operations

    size_t current_offset; // Tracks the current offset in the pool
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
        delete memory_pool;
        memory_pool = nullptr;
    }

private:
    static MemoryPool *memory_pool;
};
