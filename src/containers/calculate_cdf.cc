#include "containers/calculate_cdf.hpp"

void get_and_sort_freq(const std::unordered_map<block_id_t, size_t> &perf_map, CDFType &cdf_result)
{
    uint64_t total_keys = 0;
    // Find the maximum block_id to determine total_keys
    for (const auto &kv : perf_map)
    {
        if (kv.first > total_keys)
        {
            total_keys = kv.first;
        }
    }

    // Initialize key_map to track existing keys
    std::vector<bool> key_map(total_keys + 1, false);

    // Create a vector of (frequency, block_id) pairs
    std::vector<std::pair<uint64_t, block_id_t>> sorted_key_freq;
    for (const auto &kv : perf_map)
    {
        sorted_key_freq.push_back(std::make_pair(kv.second, kv.first));
        key_map[kv.first] = true;
    }

    // Add missing keys with frequency 0
    for (uint64_t i = 1; i <= total_keys; ++i)
    {
        if (!key_map[i])
        {
            sorted_key_freq.push_back(std::make_pair(0, i));
        }
    }

    // Sort in descending order of frequency
    std::sort(sorted_key_freq.begin(), sorted_key_freq.end(),
              [](const std::pair<uint64_t, block_id_t> &a, const std::pair<uint64_t, block_id_t> &b)
              {
                  return a.first > b.first;
              });

    // Prepare the CDF result
    auto &sorted_key_freqs = cdf_result.first;
    auto &key_freq_bucket_map = cdf_result.second;
    sorted_key_freqs.clear();
    key_freq_bucket_map.clear();

    uint64_t total_freq = 0;
    for (const auto &kv : sorted_key_freq)
    {
        total_freq += kv.first;
    }

    uint64_t cumulative_freq = 0;
    std::map<uint64_t, std::vector<std::pair<uint64_t, block_id_t>>> cdf_buckets;
    for (const auto &kv : sorted_key_freq)
    {
        cumulative_freq += kv.first;
        uint64_t percentile = (total_freq > 0) ? (cumulative_freq * 100) / total_freq : 0;
        cdf_buckets[percentile].push_back(kv);
    }

    // Sort keys within each bucket
    uint64_t total_cum_sum = 0;
    for (auto &bucket : cdf_buckets)
    {
        auto &bucket_keys = bucket.second;
        std::sort(bucket_keys.begin(), bucket_keys.end(),
                  [](const std::pair<uint64_t, block_id_t> &a, const std::pair<uint64_t, block_id_t> &b)
                  {
                      return a.second > b.second;
                  });
        for (const auto &it : bucket_keys)
        {
            sorted_key_freqs.push_back(std::make_tuple(it.first, it.second, bucket.first));
            total_cum_sum += it.first;
            key_freq_bucket_map[it.second] = std::make_pair(total_cum_sum, bucket.first);
        }
    }
}

// Function to get the sum of frequencies between two indices
uint64_t get_sum_freq_till_index(const CDFType &cdf, uint64_t start, uint64_t end)
{
    uint64_t sum = 0;
    const auto &key_freq_bucket_map = cdf.second;
    const auto &sorted_key_freqs = cdf.first;

    if (start >= sorted_key_freqs.size())
    {
        start = sorted_key_freqs.size() - 1;
    }
    if (end >= sorted_key_freqs.size())
    {
        end = sorted_key_freqs.size() - 1;
    }

    uint64_t start_cum_freq = key_freq_bucket_map.at(std::get<1>(sorted_key_freqs[start])).first;
    uint64_t end_cum_freq = key_freq_bucket_map.at(std::get<1>(sorted_key_freqs[end])).first;
    return end_cum_freq - start_cum_freq;
}

// Function to calculate performance based on watermarks and latencies
uint64_t calculate_performance(const CDFType &cdf, uint64_t water_mark_local, uint64_t water_mark_remote,
                               uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg)
{
    uint64_t total_keys = cdf.first.size();
    uint64_t total_local_accesses = get_sum_freq_till_index(cdf, 0, water_mark_local);
    uint64_t total_remote_accesses = get_sum_freq_till_index(cdf, water_mark_local, water_mark_local + water_mark_remote);
    uint64_t total_disk_accesses = get_sum_freq_till_index(cdf, water_mark_local + water_mark_remote, total_keys - 1);

    uint64_t local_latency = total_local_accesses * cache_ns_avg;
    uint64_t remote_latency = total_remote_accesses * rdma_ns_avg;
    uint64_t disk_latency = total_disk_accesses * disk_ns_avg;

    uint64_t total_latency = local_latency + remote_latency + disk_latency;
    uint64_t performance = (total_latency != 0) ? (UINT64_MAX / total_latency) : 0;

    return performance;
}

// Helper function to convert percentage to index
size_t percentage_to_index(size_t total_size, float percent)
{
    return static_cast<size_t>(total_size * (percent / 100.0));
}

// Function to determine the best access rates
void get_best_access_rates(const std::unordered_map<block_id_t, size_t> &perf_map, CDFType &cdf,
                           uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg, uint64_t cache_size_,
                           std::unordered_map<block_id_t, size_t> &keys_that_can_be_admitted)
{
    std::cout << "\nCalculating best access rates" << std::endl;
    get_and_sort_freq(perf_map, cdf);
    uint64_t cache_size = cache_size_;
    uint64_t best_performance = 0;
    uint64_t best_water_mark_local = 0;
    uint64_t best_water_mark_remote = cache_size;
    if (rdma_ns_avg == 0)
    {
        rdma_ns_avg = 10000;
    }

    for (uint64_t i = 0; i < cache_size; ++i)
    {
        uint64_t local = i;
        uint64_t remote = cache_size - (3 * local);
        if (remote < 0)
        {
            break;
        }
        if (local > cache_size / 3)
        {
            break;
        }
        uint64_t new_performance = calculate_performance(cdf, local, remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
        if (new_performance > best_performance)
        {
            best_performance = new_performance;
            best_water_mark_local = local;
            best_water_mark_remote = remote;
        }
    }

    std::cout << "Best local: " << best_water_mark_local << ", Best remote: " << best_water_mark_remote
              << ", Best performance: " << best_performance << std::endl;

    // keys_that_can_be_admitted.clear();
    for (uint64_t i = 0; i < best_water_mark_local; ++i)
    {
        keys_that_can_be_admitted[std::get<1>(cdf.first[i])] = std::get<0>(cdf.first[i]);
    }
}

// Function to print the CDF for debugging purposes
void print_cdf(const CDFType &cdf, uint64_t file_number)
{
    auto file_name = "cdf_output" + std::to_string(file_number) + ".txt";
    std::ofstream file(file_name);
    for (const auto &it : cdf.first)
    {
        file << std::get<0>(it) << " " << std::get<1>(it) << " " << std::get<2>(it) << std::endl;
    }
    file.close();
}