#pragma once
#ifndef ACCESS_RATE_CALCULATIONS_H
#define ACCESS_RATE_CALCULATIONS_H

#include <unordered_map>
#include <vector>
#include <tuple>
#include <map>
#include <cstdint>
#include <algorithm>
#include <iostream>
#include <fstream>

// Define block_id_t as uint64_t for simplicity
using block_id_t = uint64_t;

// Define CDFType as a pair of a vector and a map
using CDFType = std::pair<
    std::vector<std::tuple<uint64_t, block_id_t, uint64_t>>,
    std::map<block_id_t, std::pair<uint64_t, uint64_t>>>;

// Function to get and sort frequencies from perf_map
void get_and_sort_freq(const std::unordered_map<block_id_t, size_t> &perf_map, CDFType &cdf_result);

// Function to get the sum of frequencies between two indices
uint64_t get_sum_freq_till_index(const CDFType &cdf, uint64_t start, uint64_t end);

// Function to calculate performance based on watermarks and latencies
uint64_t calculate_performance(const CDFType &cdf, uint64_t water_mark_local, uint64_t water_mark_remote,
                               uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg);

// Helper function to convert percentage to index
size_t percentage_to_index(size_t total_size, float percent);

// Function to determine the best access rates
void get_best_access_rates(const std::unordered_map<block_id_t, size_t> &perf_map, CDFType &cdf,
                           uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg, uint64_t cache_size_,
                           std::unordered_map<block_id_t, size_t> &keys_that_can_be_admitted);

// Function to print the CDF for debugging purposes
void print_cdf(const CDFType &cdf, uint64_t file_number);

#endif // ACCESS_RATE_CALCULATIONS_H
