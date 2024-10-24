#pragma once

#include <cstdlib>
#include <unordered_map>

#include <iostream>
#include <cstring>
#include <mutex>
#include <cstddef>  // for size_t
#include <unistd.h> // for sysconf

typedef uint64_t block_id_t;

class PageMap
{
public:
    PageMap() = default;

    // Add an entry to the map
    void add_to_map(block_id_t block_id, size_t offset)
    {
        std::lock_guard<std::mutex> lock(map_mutex);
        block_offset_map[block_id] = offset;
        std::cout << "Added block_id " << block_id << " with offset " << offset << " to the map." << std::endl;
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
            return -1;
        }
    }

private:
    std::unordered_map<block_id_t, size_t> block_offset_map; // Map of block_id -> offset
    std::mutex map_mutex;                                    // Protects access to the map
};
