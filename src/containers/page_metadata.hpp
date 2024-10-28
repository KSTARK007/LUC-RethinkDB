#pragma once

#include <cstdlib>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <cstring>
#include <mutex>
#include <cstddef>  // for size_t
#include <unistd.h> // for sysconf

#define PRINT_MAP_FREQ 10000 // Frequency of printing map contents
typedef uint64_t block_id_t;

class PageMap
{
public:
    explicit PageMap() : operation_count(0), print_frequency(PRINT_MAP_FREQ) {}

    // Add an entry to the map
    void add_to_map(block_id_t block_id, size_t offset)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        block_offset_map[block_id] = offset;
        std::cout << "Added block_id " << block_id << " with offset " << offset << " to the map." << std::endl;

        increment_operation_count();
    }

    // Remove an entry from the map
    void remove_from_map(block_id_t block_id)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        if (block_offset_map.find(block_id) != block_offset_map.end())
        {
            block_offset_map.erase(block_id);
            std::cout << "Removed block_id " << block_id << " from the map." << std::endl;
        }
        else
        {
            std::cerr << "block_id " << block_id << " not found in the map." << std::endl;
        }

        increment_operation_count();
    }

    // Retrieve the offset for a given block_id
    size_t get_offset(block_id_t block_id)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        if (block_offset_map.find(block_id) != block_offset_map.end())
        {
            return block_offset_map[block_id];
        }
        else
        {
            std::cerr << "block_id " << block_id << " not found in the map." << std::endl;
            return static_cast<size_t>(-1); // Use static_cast to avoid warnings
        }
    }

private:
    std::unordered_map<block_id_t, size_t> block_offset_map; // Map of block_id -> offset
    std::mutex map_mutex;                                    // Protects access to the map
    size_t operation_count;                                  // Tracks the number of operations
    const size_t print_frequency;                            // Frequency of printing map contents

    // Increment the operation count and print map if the count reaches the print frequency
    void increment_operation_count()
    {
        operation_count++;
        if (operation_count >= print_frequency)
        {
            print_map_to_file();
            operation_count = 0; // Reset the count after printing
        }
    }

    // Print the map contents to a file
    void print_map_to_file()
    {
        std::ofstream outfile("page_map_output.txt", std::ios_base::app); // Append mode
        if (!outfile)
        {
            std::cerr << "Failed to open file for writing." << std::endl;
            return;
        }

        outfile << "Current map contents:\n";
        for (const auto &entry : block_offset_map)
        {
            outfile << "block_id: " << entry.first << ", offset: " << entry.second << "\n";
        }
        outfile << "--------------------------\n";
        outfile.close();
        std::cout << "Map contents written to file." << std::endl;
    }
};
