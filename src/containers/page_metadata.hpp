#pragma once

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <cstring>
#include <mutex>
#include <sstream>
#include <cstddef>  // for size_t
#include <unistd.h> // for sysconf
#include <thread>
#include <containers/rdma.hpp>
#include <containers/json_traversal.hpp>

#define PRINT_MAP_FREQ 10000 // Frequency of printing map contents
// #define MAX_METADATA_SIZE (uint64_t)(100) * 1024 * 1024 // 100 MB
// #define MAX_METADATA_BLOCKS 100000 // Fixed array size for block offset map
#define ALIGNMENT 64 // Alignment boundary for memory allocation
#define META_DATA_PORT 6000

typedef uint64_t block_id_t;

class PageMap
{
public:
    explicit PageMap()
        : file_number(0),
          rdma_connection("10.10.1.1", 0, true),
          block_offset_map(nullptr)
    {
        // Allocate aligned memory for the block_offset_map array
        if (posix_memalign(reinterpret_cast<void **>(&block_offset_map), ALIGNMENT, MAX_METADATA_BLOCKS * sizeof(size_t)) != 0)
        {
            std::cerr << "Memory allocation failed for block_offset_map." << std::endl;
            std::exit(EXIT_FAILURE); // Exit if memory allocation fails
        }

        // Initialize the array to an invalid offset to indicate unused entries
        std::fill_n(block_offset_map, MAX_METADATA_BLOCKS, static_cast<size_t>(-1));

        port_number = current_port++;
    }

    void create_PageMap(int tmp)
    {
        file_number = 0;
        RDMAServer rdma_connection("10.10.1.1", 0, true);
        block_offset_map = nullptr;
    }

    PageMap(int tmp)
        : file_number(0),
          rdma_connection("10.10.1.1", 0, true),
          block_offset_map(nullptr)
    {

        // Allocate aligned memory for the block_offset_map array
        if (posix_memalign(reinterpret_cast<void **>(&block_offset_map), ALIGNMENT, MAX_METADATA_BLOCKS * sizeof(size_t)) != 0)
        {
            std::cerr << "Memory allocation failed for block_offset_map." << std::endl;
            std::exit(EXIT_FAILURE); // Exit if memory allocation fails
        }

        // Initialize the array to an invalid offset to indicate unused entries
        std::fill_n(block_offset_map, MAX_METADATA_BLOCKS, static_cast<size_t>(-1));
    }

    ~PageMap()
    {
        // Free the allocated memory
        if (block_offset_map != nullptr)
        {
            free(block_offset_map);
        }
    }

    // Add an entry to the map
    void add_to_map(block_id_t block_id, size_t offset)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        if (block_id < MAX_METADATA_BLOCKS)
        {
            block_offset_map[block_id] = offset;
            // std::cout << "Added block_id " << block_id << " with offset " << offset << " to the map." << std::endl;
        }
        else
        {
            std::cerr << "block_id " << block_id << " is out of bounds." << std::endl;
        }
    }

    // Remove an entry from the map
    void remove_from_map(block_id_t block_id)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        if (block_id < MAX_METADATA_BLOCKS && block_offset_map[block_id] != static_cast<size_t>(-1))
        {
            block_offset_map[block_id] = static_cast<size_t>(-1); // Reset to invalid offset
            // std::cout << "Removed block_id " << block_id << " from the map." << std::endl;
        }
        // else
        // {
        //     std::cerr << "block_id " << block_id << " not found in the map." << std::endl;
        // }
    }

    // Retrieve the offset for a given block_id
    size_t get_offset_from_map(block_id_t block_id)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        if (block_id < MAX_METADATA_BLOCKS && block_offset_map[block_id] != static_cast<size_t>(-1))
        {
            return block_offset_map[block_id];
        }
        else
        {
            std::cerr << "block_id " << block_id << " not found in the map." << std::endl;
            return static_cast<size_t>(-1); // Return an invalid offset
        }
    }

    void print_map_to_file(size_t file_number)
    {
        // std::lock_guard<std::mutex> lock(map_mutex);
        std::stringstream ss;
        ss << "page_map_output" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream outfile(file_name, std::ios_base::app); // Append mode
        if (!outfile)
        {
            std::cerr << "Failed to open file for writing." << std::endl;
            return;
        }

        if (block_offset_map == nullptr)
        {
            std::cerr << "block_offset_map is null." << std::endl;
            return;
        }

        outfile << "Current map contents:\n";
        for (block_id_t i = 0; i < MAX_METADATA_BLOCKS; ++i)
        {
            // std::cout << "block_id: " << i << std::endl;
            // std::cout << ", offset: " << block_offset_map[i] << std::endl;
            if (block_offset_map[i] != static_cast<size_t>(-1))
            {
                outfile << "block_id: " << i << ", offset: " << block_offset_map[i] << "\n";
            }
        }
        outfile << "--------------------------\n";
        outfile.close();
        std::cout << "Map contents written to file." << std::endl;
    }

    void print_map_to_file_remote_metadata(std::string ip, size_t file_number)
    {
        // std::lock_guard<std::mutex> lock(map_mutex);
        std::stringstream ss;
        ss << "page_map_output_remote_ip" << ip << "_filenumber" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream outfile(file_name, std::ios_base::app); // Append mode
        if (!outfile)
        {
            std::cerr << "Failed to open file for writing." << std::endl;
            return;
        }

        if (block_offset_map == nullptr)
        {
            std::cerr << "block_offset_map is null." << std::endl;
            return;
        }

        outfile << "Current map contents:\n";
        for (block_id_t i = 0; i < MAX_METADATA_BLOCKS; ++i)
        {
            // std::cout << "block_id: " << i << std::endl;
            // std::cout << ", offset: " << block_offset_map[i] << std::endl;
            if (block_offset_map[i] != static_cast<size_t>(-1))
            {
                outfile << "block_id: " << i << ", offset: " << block_offset_map[i] << "\n";
            }
        }
        outfile << "--------------------------\n";
        outfile.close();
        std::cout << "Map contents written to file." << std::endl;
    }

    void print_block_offset_map(void *new_map)
    {
        std::stringstream ss;
        ss << "remote_page_out" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream outfile(file_name, std::ios_base::app); // Append mode
        if (!outfile)
        {
            std::cerr << "Failed to open file for writing." << std::endl;
            return;
        }
        size_t *tmp_map = static_cast<size_t *>(new_map);
        if (tmp_map == nullptr)
        {
            std::cerr << "block_offset_map is null." << std::endl;
            return;
        }

        std::lock_guard<std::mutex> lock(map_mutex);
        for (block_id_t i = 0; i < MAX_METADATA_BLOCKS; ++i)
        {
            if (tmp_map[i] != static_cast<size_t>(-1))
            {
                outfile << "block_id: " << i << ", offset: " << tmp_map[i] << std::endl;
            }
        }
    }

    void update_block_offset_map(void **new_map)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        void *temp = block_offset_map;
        block_offset_map = static_cast<size_t *>(*new_map);
        *new_map = temp;
    }

    size_t isBlockIDAvailable(block_id_t block_id)
    {
        // std::lock_guard<std::mutex> lock(map_mutex);
        if (block_offset_map[block_id] != static_cast<size_t>(-1) && block_offset_map[block_id] != 0)
        {
            return block_offset_map[block_id];
        }
        return 0;
    }

    bool updateBlockID(block_id_t block_id, size_t offset)
    {
        // std::lock_guard<std::mutex> lock(map_mutex);
        if (block_offset_map[block_id] != static_cast<size_t>(-1))
        {
            block_offset_map[block_id] = offset;
            return true;
        }
        return false;
    }

    size_t file_number;
    RDMAServer rdma_connection;
    int port_number; // Instance-specific port number

    size_t *block_offset_map; // Pointer to aligned array for block_id -> offset
    std::mutex map_mutex;     // Protects access to the map
    static int current_port;  // Static variable to track the current port number across instances
};