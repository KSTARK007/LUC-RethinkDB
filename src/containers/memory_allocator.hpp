#include <cstdlib>
#include <iostream>
#include <cstring>
#include <mutex>
#include <cstddef>  // for size_t
#include <unistd.h> // for sysconf
#include <containers/page_metadata.hpp>
#include <iomanip> // for std::hex, std::setw, std::setfill

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
    void *allocate(size_t size)
    {
        std::lock_guard<std::mutex> lock(pool_mutex); // Thread-safe access

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

    // Deallocate function stub
    void deallocate(void *ptr)
    {
        if (ptr == nullptr)
        {
            return;
        }
        std::cout << "Deallocated memory from aligned memory pool at address " << ptr << std::endl;
    }

    uint64_t get_offset(void *ptr)
    {
        std::cout << "Getting offset for memory at address " << ptr << std::endl;
        return static_cast<uint64_t>((char *)ptr - memory);
    }

    // Read and print the content of a block given an offset and size
    void read_block(uint64_t offset, size_t size)
    {
        std::lock_guard<std::mutex> lock(pool_mutex); // Ensure thread-safe access
        if (offset + size > static_cast<size_t>(pool_end - memory))
        {
            std::cerr << "Invalid read: Out of bounds for offset " << offset << " with size " << size << std::endl;
            return;
        }

        char *block_start = memory + offset;
        std::cout << "Reading block at offset " << offset << " with size " << size << " bytes:" << std::endl;
        for (size_t i = 0; i < size; ++i)
        {
            std::cout << std::hex << std::setw(2) << std::setfill('0') << (static_cast<int>(block_start[i]) & 0xff) << " ";
            if ((i + 1) % 16 == 0) // New line every 16 bytes for readability
            {
                std::cout << std::endl;
            }
        }
        std::cout << std::dec << std::endl; // Reset to decimal format
    }

    // Print the content of a block given a pointer and size
    void print_block_content(void *ptr, size_t size)
    {
        std::lock_guard<std::mutex> lock(pool_mutex); // Ensure thread-safe access

        char *block_start = static_cast<char *>(ptr);
        std::cout << "Printing block content at address " << ptr << " with size " << size << " bytes:" << std::endl;
        for (size_t i = 0; i < size; ++i)
        {
            std::cout << std::hex << std::setw(2) << std::setfill('0') << (static_cast<int>(block_start[i]) & 0xff) << " ";
            if ((i + 1) % 16 == 0) // New line every 16 bytes for readability
            {
                std::cout << std::endl;
            }
        }
        std::cout << std::dec << std::endl; // Reset to decimal format
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

    uint64_t get_offset(void *ptr)
    {
        return memory_pool->get_offset(ptr);
    }

    static MemoryPool *memory_pool;
};
