#include "containers/memory_allocator.hpp"
#include "containers/rdma.hpp"

MemoryPool *PageAllocator::memory_pool = nullptr;

MemoryPool::MemoryPool(size_t pool_size, size_t alignment)
    : rdma_connection("10.10.1.1", 0, true) // IP and index can be adjusted as needed
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

    // Initialize RDMA connection
    rdma_connection.init(memory, pool_size, SERVER_PORT);
}

// Destructor
MemoryPool::~MemoryPool()
{
    free(memory);
    std::cout << "Memory pool destroyed." << std::endl;
}

// Allocate memory from the aligned pool
void *MemoryPool::allocate(size_t size)
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
void MemoryPool::deallocate(void *ptr)
{
    if (ptr == nullptr)
    {
        return;
    }
    // std::cout << "Deallocated memory from aligned memory pool at address " << ptr << std::endl;
}

// Get offset of a pointer within the memory pool
uint64_t MemoryPool::get_offset(void *ptr)
{
    std::cout << "Getting offset for memory at address " << ptr << std::endl;
    return static_cast<uint64_t>(static_cast<char *>(ptr) - memory);
}

// Read and print the content of a block given an offset and size
void MemoryPool::read_block(uint64_t offset, size_t size)
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
void MemoryPool::print_block_content(void *ptr, size_t size)
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

void MemoryPool::populate_block()
{
    // std::lock_guard<std::mutex> lock(pool_mutex); // Ensure thread-safe access

    size_t block_size = 4 * 1024; // 4 KB block size
    int num_blocks = 1000;        // Number of blocks to allocate

    // Random character generator setup
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<char> dis('A', 'Z'); // Random characters from 'A' to 'Z'

    std::cout << "Populating memory pool with random blocks:" << std::endl;
    for (int i = 0; i < num_blocks; ++i)
    {
        // Allocate a 4 KB block from the memory pool
        char *block = static_cast<char *>(allocate(block_size));
        if (block == nullptr)
        {
            std::cerr << "Failed to allocate memory for block " << i << std::endl;
            continue;
        }

        // Fill the block with random characters
        std::cout << "Populating block " << i << " with random characters:" << std::endl;
        for (size_t j = 0; j < block_size; ++j)
        {
            block[j] = dis(gen);
        }

        std::cout << "Block " << i << " populated with random characters at address " << static_cast<void *>(block) << std::endl;
    }
}