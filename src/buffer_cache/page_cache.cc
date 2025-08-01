// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "buffer_cache/page_cache.hpp"

#include <inttypes.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <stack>
#include <iostream>

#include "arch/runtime/coroutines.hpp"
#include "arch/runtime/runtime.hpp"
#include "arch/runtime/runtime_utils.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/new_mutex.hpp"
#include "buffer_cache/cache_balancer.hpp"
#include "do_on_thread.hpp"
#include "serializer/serializer.hpp"
#include "stl_utils.hpp"

cache_conn_t::~cache_conn_t()
{
    // The user could only be expected to make sure that txn_t objects don't have
    // their lifetime exceed the cache_conn_t's.  Soft durability makes it possible
    // that the inner page_txn_t's lifetime would exceed the cache_conn_t's.  So we
    // need to tell the page_txn_t that we don't exist -- we do so by nullating its
    // cache_conn_ pointer (which it's capable of handling).
    if (newest_txn_ != nullptr)
    {
        newest_txn_->cache_conn_ = nullptr;
        newest_txn_ = nullptr;
    }
}

namespace alt
{

    class current_page_help_t
    {
    public:
        current_page_help_t(block_id_t _block_id, page_cache_t *_page_cache)
            : block_id(_block_id), page_cache(_page_cache) {}
        block_id_t block_id;
        page_cache_t *page_cache;
    };

    void throttler_acq_t::update_dirty_page_count(int64_t new_count)
    {
        rassert(
            block_changes_semaphore_acq_.count() == index_changes_semaphore_acq_.count());
        if (new_count > block_changes_semaphore_acq_.count())
        {
            block_changes_semaphore_acq_.change_count(new_count);
            index_changes_semaphore_acq_.change_count(new_count);
        }
    }

    void throttler_acq_t::mark_dirty_pages_written()
    {
        block_changes_semaphore_acq_.change_count(0);
    }

    page_read_ahead_cb_t::page_read_ahead_cb_t(serializer_t *serializer,
                                               page_cache_t *page_cache)
        : serializer_(serializer), page_cache_(page_cache)
    {
        serializer_->register_read_ahead_cb(this);
    }

    page_read_ahead_cb_t::~page_read_ahead_cb_t() {}

    void page_read_ahead_cb_t::offer_read_ahead_buf(
        block_id_t block_id,
        buf_ptr_t *buf,
        const counted_t<standard_block_token_t> &token)
    {
        assert_thread();
        buf_ptr_t local_buf = std::move(*buf);

        block_size_t block_size = block_size_t::undefined();
        scoped_device_block_aligned_ptr_t<ser_buffer_t> ptr;
        local_buf.release(&block_size, &ptr);

        // We're going to reconstruct the buf_ptr_t on the other side of this do_on_thread
        // call, so we'd better make sure the block size is right.
        guarantee(block_size.value() == token->block_size().value());

        // Notably, this code relies on do_on_thread to preserve callback order (which it
        // does do).
        do_on_thread(page_cache_->home_thread(),
                     std::bind(&page_cache_t::add_read_ahead_buf,
                               page_cache_,
                               block_id,
                               copyable_unique_t<scoped_device_block_aligned_ptr_t<ser_buffer_t>>(std::move(ptr)),
                               token));
    }

    void page_read_ahead_cb_t::destroy_self()
    {
        serializer_->unregister_read_ahead_cb(this);
        serializer_ = nullptr;

        page_cache_t *page_cache = page_cache_;
        page_cache_ = nullptr;

        do_on_thread(page_cache->home_thread(),
                     std::bind(&page_cache_t::read_ahead_cb_is_destroyed, page_cache));

        // Self-deletion.  Old-school.
        delete this;
    }

    void page_cache_t::consider_evicting_current_page(block_id_t block_id)
    {
        ASSERT_NO_CORO_WAITING;
        // We can't do anything until read-ahead is done, because it uses the existence
        // of a current_page_t entry to figure out whether the read-ahead page could be
        // out of date.
        if (read_ahead_cb_ != nullptr)
        {
            return;
        }

        auto page_it = current_pages_.find(block_id);
        if (page_it == current_pages_.end())
        {
            return;
        }

        current_page_t *page_ptr = page_it->second;
        if (page_ptr->should_be_evicted())
        {
            page_map.remove_from_map(block_id);
            current_pages_.erase(block_id);
            page_ptr->reset(this);
            delete page_ptr;
        }
    }

    void page_cache_t::add_read_ahead_buf(block_id_t block_id,
                                          scoped_device_block_aligned_ptr_t<ser_buffer_t> ptr,
                                          const counted_t<standard_block_token_t> &token)
    {
        assert_thread();

        // We MUST stop if read_ahead_cb_ is NULL because that means current_page_t's
        // could start being destroyed.
        if (read_ahead_cb_ == nullptr)
        {
            return;
        }

        // We MUST stop if current_pages_[block_id] already exists, because that means
        // the read-ahead page might be out of date.
        if (current_pages_.count(block_id) > 0)
        {
            return;
        }

        // We know the read-ahead page is not out of date if current_pages_[block_id]
        // doesn't exist and if read_ahead_cb_ still exists -- that means a current_page_t
        // for the block id was never created, and thus the page could not have been
        // modified (not to mention that we've already got the page in memory, so there is
        // no useful work to be done).

        buf_ptr_t buf(token->block_size(), std::move(ptr));
        current_pages_[block_id] = new current_page_t(block_id, std::move(buf), token, this);
        page_t *page_instance = current_pages_[block_id]->page_.get_page_for_read();

        if (page_instance != nullptr)
        {
            void *page_buffer = page_instance->get_page_buf(this);

            if (page_buffer != nullptr)
            {
                uint64_t page_offset_tmp = PageAllocator::memory_pool->get_offset(page_buffer);
                page_map.add_to_map(block_id, page_offset_tmp);
            }
            else
            {
                std::cerr << "Error: Buffer data unavailable for block_id " << block_id << std::endl;
            }
        }
        else
        {
            page_map.add_to_map(block_id, static_cast<size_t>(-1));
        }
    }

    void page_cache_t::have_read_ahead_cb_destroyed()
    {
        assert_thread();

        if (read_ahead_cb_ != nullptr)
        {
            // By setting read_ahead_cb_ to nullptr, we make sure we only tell the read
            // ahead cb to destroy itself exactly once.
            page_read_ahead_cb_t *cb = read_ahead_cb_;
            read_ahead_cb_ = nullptr;

            do_on_thread(cb->home_thread(),
                         std::bind(&page_read_ahead_cb_t::destroy_self, cb));

            coro_t::spawn_sometime(std::bind(&page_cache_t::consider_evicting_all_current_pages, this, drainer_->lock()));
        }
    }

    void page_cache_t::consider_evicting_all_current_pages(page_cache_t *page_cache,
                                                           auto_drainer_t::lock_t lock)
    {
        // Atomically grab a list of block IDs that currently exist in current_pages.
        std::vector<block_id_t> current_block_ids;
        current_block_ids.reserve(page_cache->current_pages_.size());
        for (const auto &current_page : page_cache->current_pages_)
        {
            current_block_ids.push_back(current_page.first);
        }

        // In a separate step, evict current pages that should be evicted.
        // We do this separately so that we can yield between evictions.
        size_t i = 0;
        for (block_id_t id : current_block_ids)
        {
            page_cache->consider_evicting_current_page(id);
            if (i % 16 == 15)
            {
                coro_t::yield();
                if (lock.get_drain_signal()->is_pulsed())
                {
                    return;
                }
            }
            ++i;
        }
    }

    void page_cache_t::read_ahead_cb_is_destroyed()
    {
        assert_thread();
        read_ahead_cb_existence_.reset();
    }

    int page_cache_t::get_node_id()
    {
        std::string tmp = PageAllocator::memory_pool->configs->my_ip;
        std::string node_id = tmp.substr(tmp.find_last_of('.') + 1);
        return std::stoi(node_id);
    }

    bool page_cache_t::check_if_node_in_range(u_int64_t block_id)
    {
        if (block_id >= start_range && block_id <= end_range)
        {
            return true;
        }
        return false;
    }

    class page_cache_index_write_sink_t
    {
    public:
        // When sink is acquired, we get in line for mutex_ right away and release the
        // sink.  The serializer_t interface uses new_mutex_t.
        fifo_enforcer_sink_t sink;
        new_mutex_t mutex;
    };
    void update_client_metadata(RDMAClient *client)
    {
        if (client == nullptr)
        {
            std::cerr << "Error: client is null at the start of update_client_metadata." << std::endl;
            return;
        }

        int file_number = 0;
        while (true)
        {
            // std::cout << "Updating metadata for client" << std::endl;

            client->readMetadata();
            // client->cleanupFrequencyMap();

            if (client->getMetaDataTmpBuffer() == nullptr)
            {
                std::cerr << "Error: MetaDataTmpBuffer is null." << std::endl;
                break;
            }

            // client->updateMetaDataBuffer();
            client->getPageMap()->update_block_offset_map(client->getMetaDataBuffer());

            // std::this_thread::sleep_for(std::chrono::microseconds(500));
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            // std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        // while (true)
        // {
        //     std::cout << "Updating metadata for client" << std::endl;
        //     client->readMetadata();
        //     client->updateMetaDataBuffer();
        //     client->getPageMap()->update_block_offset_map(client->getMetaDataTmpBuffer());
        //     // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        //     // client->readMetadata();
        //     // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        //     // std::cout << "Time taken to RDMA read metadata: " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << " us" << std::endl;
        //     // // client->getPageMap()->print_block_offset_map(client->getMetaDataBuffer());
        //     // begin = std::chrono::steady_clock::now();
        //     // client->updateMetaDataBuffer();
        //     // end = std::chrono::steady_clock::now();
        //     // std::cout << "Time taken to updateMetadata: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns" << std::endl;
        //     // begin = std::chrono::steady_clock::now();
        //     // client->getPageMap()->update_block_offset_map(client->getMetaDataTmpBuffer());
        //     // end = std::chrono::steady_clock::now();
        //     // std::cout << "Time taken to update block offset map: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns" << std::endl;
        //     // {
        //     //     client->getPageMap()->print_map_to_file(client->getPageMap()->file_number++);
        //     // }
        //     std::this_thread::sleep_for(std::chrono::seconds(1));
        // }
    }
    page_cache_t::page_cache_t(serializer_t *_serializer,
                               cache_balancer_t *balancer,
                               alt_txn_throttler_t *throttler)
        : max_block_size_(_serializer->max_block_size()),
          serializer_(_serializer),
          free_list_(_serializer),
          evicter_(),
          read_ahead_cb_(nullptr),
          drainer_(make_scoped<auto_drainer_t>())
    {
        std::cout << "Page cache created \n max_block_size_ = " << max_block_size_.value() << std::endl;
        const bool start_read_ahead = balancer->read_ahead_ok_at_start();
        if (start_read_ahead)
        {
            read_ahead_cb_existence_ = drainer_->lock();
        }

        latency_info_.disk = 100000;
        latency_info_.cache = 1000;
        latency_info_.RDMA = 5000;

        RDMA_hits_.store(0);
        int node_id = get_node_id();
        std::cout << "Node ID: " << node_id << std::endl;
        if (node_id == 1)
        {
            start_range = 0;
            end_range = static_cast<int>(1.0 / 3 * MAX_BLOCKS);
        }
        else if (node_id == 2)
        {
            start_range = static_cast<int>(1.0 / 3 * MAX_BLOCKS) + 1;
            end_range = static_cast<int>(2.0 / 3 * MAX_BLOCKS);
        }
        else if (node_id == 3)
        {
            start_range = static_cast<int>(2.0 / 3 * MAX_BLOCKS) + 1;
            end_range = MAX_BLOCKS;
        }

        page_read_ahead_cb_t *local_read_ahead_cb = nullptr;
        {
            on_thread_t thread_switcher(_serializer->home_thread());
            if (start_read_ahead)
            {
                local_read_ahead_cb = new page_read_ahead_cb_t(_serializer, this);
            }
            default_reads_account_.init(_serializer->home_thread(),
                                        _serializer->make_io_account(CACHE_READS_IO_PRIORITY));
            index_write_sink_.init(new page_cache_index_write_sink_t);
            recencies_ = _serializer->get_all_recencies();
        }

        ASSERT_NO_CORO_WAITING;
        // We don't want to accept read-ahead buffers (or any operations) until the
        // evicter is ready.  So we set read_ahead_cb_ here so that we accept read-ahead
        // buffers at exactly the same time that we initialize the evicter.  We
        // initialize the read_ahead_cb_ after the evicter_ because that way reentrant
        // usage by the balancer (before page_cache_t construction completes) would be
        // more likely to trip an assertion.
        evicter_.initialize(this, balancer, throttler);
        read_ahead_cb_ = local_read_ahead_cb;
        operation_count = 0;
        file_number = 0;
        if (page_map.port_number == 6001)
        {
            std::cout << "Initializing RDMA server on port " << page_map.port_number << std::endl;
            int pool_size = MAX_METADATA_BLOCKS * sizeof(size_t);
            int expected_connections = PageAllocator::memory_pool->configs->get_hosts().size();
            std::thread server_thread([this, pool_size, expected_connections]()
                                      { page_map.rdma_connection.init(page_map.block_offset_map, pool_size, page_map.port_number, expected_connections); });
            server_thread.detach();

            std::cout << "Remote Clients for Meta Data connection." << std::endl;
            for (const auto &host_info : PageAllocator::memory_pool->configs->get_hosts())
            {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                const std::string &host_ip = host_info.host;
                int metadata_port = host_info.metadata_port;

                // Connect to the remote metadata server
                RDMAClient *client = new RDMAClient(host_ip, metadata_port, true);
                if (client->connectToServer())
                {
                    PageMap *page_map = new PageMap(0);
                    client->setPageMap(page_map);
                    std::cout << "Connected to remote metadata server at IP: " << host_ip << ", port: " << metadata_port << std::endl;
                    PageAllocator::memory_pool->RemoteMetadata.push_back(client);

                    std::thread update_thread(update_client_metadata, client);
                    update_thread.detach();
                }
                else
                {
                    std::cerr << "Failed to connect to remote metadata server at IP: " << host_ip << ", port: " << metadata_port << std::endl;
                }
            }
        }
    }

    page_cache_t::~page_cache_t()
    {
        std::cout << "misses_ = " << misses_ << std::endl;
        assert_thread();
        // PageAllocator::destroy_pool();

        have_read_ahead_cb_destroyed();

        drainer_.reset();
        size_t i = 0;
        for (auto &&page : current_pages_)
        {
            if (i % 256 == 255)
            {
                coro_t::yield();
            }
            ++i;
            page.second->reset(this);
            delete page.second;
        }

        {
            /* IO accounts and a few other fields must be destroyed on the serializer
            thread. */
            on_thread_t thread_switcher(serializer_->home_thread());
            // Resetting default_reads_account_ is opportunistically done here, instead
            // of making its destructor switch back to the serializer thread a second
            // time.
            default_reads_account_.reset();
            index_write_sink_.reset();
        }
    }

    // We go a bit old-school, with a self-destroying callback.
    class flush_and_destroy_txn_waiter_t : public signal_t::subscription_t
    {
    public:
        flush_and_destroy_txn_waiter_t(auto_drainer_t::lock_t &&lock,
                                       page_txn_t *txn,
                                       std::function<void(throttler_acq_t *)> on_flush_complete)
            : lock_(std::move(lock)),
              txn_(txn),
              on_flush_complete_(std::move(on_flush_complete)) {}

    private:
        void run()
        {
            // Tell everybody without delay that the flush is complete.
            on_flush_complete_(&txn_->throttler_acq_);

            // We have to do the rest _later_ because of signal_t::subscription_t not
            // allowing reentrant signal_t::subscription_t::reset() calls, and the like,
            // even though it would be valid.
            // We are using `call_later_on_this_thread` instead of spawning a coroutine
            // to reduce memory overhead.
            class kill_later_t : public linux_thread_message_t
            {
            public:
                explicit kill_later_t(flush_and_destroy_txn_waiter_t *self) : self_(self) {}
                void on_thread_switch()
                {
                    self_->kill_ourselves();
                    delete this;
                }

            private:
                flush_and_destroy_txn_waiter_t *self_;
            };
            call_later_on_this_thread(new kill_later_t(this));
        }

        void kill_ourselves()
        {
            // We can't destroy txn_->flush_complete_cond_ until we've reset our
            // subscription, because computers.
            reset();
            delete txn_;
            delete this;
        }

        auto_drainer_t::lock_t lock_;
        page_txn_t *txn_;
        std::function<void(throttler_acq_t *)> on_flush_complete_;

        DISABLE_COPYING(flush_and_destroy_txn_waiter_t);
    };

    void page_cache_t::flush_and_destroy_txn(
        scoped_ptr_t<page_txn_t> txn,
        std::function<void(throttler_acq_t *)> on_flush_complete)
    {
        guarantee(txn->live_acqs_ == 0,
                  "A current_page_acq_t lifespan exceeds its page_txn_t's.");
        guarantee(!txn->began_waiting_for_flush_);

        txn->announce_waiting_for_flush();

        page_txn_t *page_txn = txn.release();
        flush_and_destroy_txn_waiter_t *sub = new flush_and_destroy_txn_waiter_t(drainer_->lock(), page_txn,
                                                                                 std::move(on_flush_complete));

        sub->reset(&page_txn->flush_complete_cond_);
    }

    void page_cache_t::end_read_txn(scoped_ptr_t<page_txn_t> txn)
    {
        guarantee(txn->touched_pages_.empty());
        guarantee(txn->live_acqs_ == 0,
                  "A current_page_acq_t lifespan exceeds its page_txn_t's.");
        guarantee(!txn->began_waiting_for_flush_);

        txn->flush_complete_cond_.pulse();
    }

    bool page_cache_t::check_block_info_map_if_leaf(block_id_t block_id)
    {
        if (block_info_map.find(block_id) == block_info_map.end())
        {
            return false;
        }
        return block_info_map[block_id].is_leaf;
    }

    void page_cache_t::update_block_info_map(block_id_t block_id, bool is_leaf, bool hit, bool miss, bool RDMA_hit)
    {
        if (block_info_map.find(block_id) == block_info_map.end())
        {
            Block_info block_info;
            block_info.is_leaf = is_leaf;
            block_info.hits = 0;
            block_info.misses = 0;
            block_info.RDMA_hit = 0;
            block_info_map[block_id] = block_info;
        }
        if (hit)
        {
            block_info_map[block_id].hits++;
        }
        if (miss)
        {
            block_info_map[block_id].misses++;
        }
        if (RDMA_hit)
        {
            block_info_map[block_id].RDMA_hit++;
        }
        if (is_leaf)
        {
            block_info_map[block_id].is_leaf = true;
        }
        if (hit || miss || RDMA_hit)
        {
            block_info_map[block_id].total_accesses++;
        }
    }

    void page_cache_t::clear_perf_map()
    {
        perf_map.clear();
    }

    void page_cache_t::print_block_info_map(size_t file_number)
    {
        std::stringstream ss;
        ss << "block_info_output" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream file;
        file.open(file_name);
        file << "Block_id, is_leaf, hits, misses, RDMA_hit, Total_access" << std::endl;
        for (auto block : block_info_map)
        {
            file << block.first << " " << block.second.is_leaf << " " << block.second.hits << " "
                 << block.second.misses << " " << block.second.RDMA_hit << " " << block.second.total_accesses << std::endl;
        }
        file.close();
    }

    bool page_cache_t::check_leaf_map_if_leaf(block_id_t block_id)
    {
        if (leaf_map.find(block_id) == leaf_map.end())
        {
            return false;
        }
        if (block_id == 0 || block_id == 1 || block_id == 2 || block_id == 3)
        {
            return true;
        }
        return true;
    }

    void page_cache_t::print_perf_map(size_t file_number)
    {
        std::stringstream ss;
        ss << "perf_map_output" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream file;
        file.open(file_name);
        file << "Block_id, Accesses" << std::endl;
        for (auto perf : perf_map)
        {
            file << perf.first << " " << perf.second << std::endl;
        }
        file.close();
    }

    void page_cache_t::update_leaf_map(block_id_t block_id, bool is_leaf)
    {
        leaf_map[block_id] = is_leaf;
    }

    void page_cache_t::print_leaf_map(size_t file_number)
    {
        std::stringstream ss;
        ss << "leaf_map_output" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream file;
        file.open(file_name);
        file << "Block_id is_leaf" << std::endl;
        for (auto leaf : leaf_map)
        {
            file << leaf.first << std::endl;
        }
        file.close();
    }

    bool page_cache_t::should_admit_block(block_id_t block_id)
    {
        if (perf_map.find(block_id) == perf_map.end())
        {
            return false;
        }
        if (perf_map[block_id] > MAX_DISK_READ_BEFORE_ADMIT)
        {
            return true;
        }
    }

    void page_cache_t::update_perf_map(block_id_t block_id)
    {
        if (check_leaf_map_if_leaf(block_id))
        {
            perf_map[block_id] = 1;
        }
        if (perf_map.find(block_id) == perf_map.end())
        {
            perf_map[block_id] = 1;
        }
        else
        {
            perf_map[block_id] += 1;
        }
    }

    void page_cache_t::print_keys_that_can_be_admitted(size_t file_number)
    {
        std::stringstream ss;
        ss << "admit_keys" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream file;
        file.open(file_name);
        file << "Block_id, Accesses" << std::endl;
        for (auto key : keys_that_can_be_admitted)
        {
            file << key.first << " " << key.second << std::endl;
        }
        file.close();
    }

    void page_cache_t::clear_keys_that_can_be_admitted()
    {
        keys_that_can_be_admitted.clear();
    }

    void page_cache_t::update_keys_that_can_be_admitted(block_id_t block_id)
    {
        if (keys_that_can_be_admitted.find(block_id) == keys_that_can_be_admitted.end())
        {
            keys_that_can_be_admitted[block_id] = 1;
        }
        else
        {
            keys_that_can_be_admitted[block_id] += 1;
        }
    }

    bool page_cache_t::check_if_key_can_be_admitted(block_id_t block_id)
    {
        if (!CBA_ENABLED)
        {
            return false;
        }
        if (keys_that_can_be_admitted.find(block_id) == keys_that_can_be_admitted.end())
        {
            return false;
        }
        total_admitted++;
        return true;
    }

    void page_cache_t::print_current_pages_to_file(size_t file_number)
    {
        std::stringstream ss;
        ss << "current_pages_output" << file_number << ".txt";
        std::string file_name = ss.str();
        std::ofstream file;
        file.open(file_name);
        uint64_t total_pages = 0;
        reset_counter();
        bool internal_page = false;
        for (auto &&page : current_pages_)
        {
            page_t *page_instance = page.second->page_.get_page_for_read();
            if (page_instance != nullptr)
            {
                if (page_instance->is_loaded())
                {
                    if (check_if_internal_page(page_instance))
                    {
                        internal_pages++;
                        update_leaf_map(page.first, true);
                        update_block_info_map(page.first, true, false, false, false);
                        internal_page = true;
                        // continue;
                    }
                }
                if (page_instance->is_rdma_page())
                {
                    rdma_bag++;
                    if (!check_leaf_map_if_leaf(page_instance->block_id()))
                    {
                        continue;
                    }
                    // continue;
                }
                else if (page_instance->is_loading() || page_instance->has_waiters())
                {
                    unevictable_bag++;
                }
                else if (!page_instance->is_loaded())
                {
                    evicted_bag++;
                }
                else if (page_instance->is_disk_backed())
                {
                    evictable_disk_backed_bag++;
                }
                else
                {
                    evictable_unbacked_bag++;
                }

                file << page.first << std::endl;
            }
            else
            {
                total_pages++;
            }
        }
        file.close();
    }

    current_page_t *page_cache_t::page_for_block_id(block_id_t block_id, bool isRead)
    {
        assert_thread();
        bool should_add_to_local_cache = false;
        bool write_key_found = false;
        uint64_t writes_hit = 0;
        auto write_page_it = write_current_pages_.find(block_id);
        if (write_page_it != write_current_pages_.end())
        {
            write_key_found = true;
            writes_hit++;
            update_perf_map(block_id);
            page_t *page_instance = write_page_it->second->page_.get_page_for_read();
            if (page_instance->is_loaded())
            {
                if (check_if_internal_page(page_instance))
                {
                    update_leaf_map(block_id, true);
                    update_block_info_map(block_id, true, false, false, false);
                }
            }
            update_block_info_map(block_id, false, true, false, false);
            rassert(!write_page_it->second->is_deleted());
        }

        auto page_it = current_pages_.find(block_id);
        if (!write_key_found)
        {
            if ((page_it == current_pages_.end()))
            {
                rassert(is_aux_block_id(block_id) ||
                            recency_for_block_id(block_id) != repli_timestamp_t::invalid,
                        "Expected block %" PR_BLOCK_ID " not to be deleted "
                        "(should you have used alt_create_t::create?).",
                        block_id);
                update_perf_map(block_id);
                // std::cout << "Block " << block_id << " not found in the cache. page_port " << page_map.port_number << std::endl;
                if (isRead && RDMA_ENABLED && PRINT_LATENCY)
                {
                    std::cout << "RDMA ENABLED and PRINT_LATENCY is true" << std::endl;
                    std::cout << "Block " << block_id << " not found in the cache. page_port " << page_map.port_number << std::endl;
                    std::pair<RDMAClient *, size_t> tmp;
                    RDMAClient *client = nullptr;
                    size_t offset = 0;
                    if (page_map.port_number == 6001)
                    {
                        auto begin = std::chrono::steady_clock::now();
                        tmp = PageAllocator::memory_pool->check_block_exists(block_id);
                        auto end = std::chrono::steady_clock::now();
                        std::cout << "Time taken for check_block_exists: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;
                        client = tmp.first;
                        offset = tmp.second;
                    }
                    if (client != nullptr && block_id != 0 && offset != static_cast<size_t>(-1))
                    {
                        uint32_t page_size = max_block_size_.value();
                        std::cout << "Block " << block_id << " exists on remote server." << client->getIP() << std::endl;

                        auto begin = std::chrono::steady_clock::now();
                        void *block_data = client->getPageFromOffset(offset, page_size);
                        auto end = std::chrono::steady_clock::now();
                        std::cout << "Time taken for getPageFromOffset: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                        if (block_data != nullptr)
                        {
                            RDMA_hits_.fetch_add(1);
                            uint32_t ser_bs = page_size + sizeof(ls_buf_data_t); // Total serialized block size
                            block_size_t block_size = block_size_t::unsafe_make(ser_bs);

                            begin = std::chrono::steady_clock::now();
                            buf_ptr_t buf = buf_ptr_t::alloc_uninitialized(block_size);
                            end = std::chrono::steady_clock::now();
                            std::cout << "Time taken for alloc_uninitialized: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                            begin = std::chrono::steady_clock::now();
                            std::memcpy(buf.cache_data(), block_data, page_size);
                            end = std::chrono::steady_clock::now();
                            std::cout << "Time taken for memcpy: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                            begin = std::chrono::steady_clock::now();
                            buf.fill_padding_zero();
                            end = std::chrono::steady_clock::now();
                            std::cout << "Time taken for fill_padding_zero: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                            begin = std::chrono::steady_clock::now();
                            current_page_t *page = new current_page_t(block_id, std::move(buf), this, true);
                            end = std::chrono::steady_clock::now();
                            std::cout << "Time taken for creating current_page_t: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                            begin = std::chrono::steady_clock::now();
                            client->addFrequencyMapEntry(block_id);
                            end = std::chrono::steady_clock::now();
                            std::cout << "Time taken for addFrequencyMapEntry: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                            if (client->performFrequencyMapLookup(block_id))
                            {
                                begin = std::chrono::steady_clock::now();
                                page_it = current_pages_.insert(page_it, std::make_pair(block_id, page));
                                end = std::chrono::steady_clock::now();
                                std::cout << "Time taken for current_pages_.insert: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                                begin = std::chrono::steady_clock::now();
                                page_t *page_instance = current_pages_[block_id]->page_.get_page_for_read();
                                end = std::chrono::steady_clock::now();
                                std::cout << "Time taken for get_page_for_read: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                                if (page_instance != nullptr)
                                {
                                    begin = std::chrono::steady_clock::now();
                                    void *page_buffer = page_instance->get_page_buf(this);
                                    end = std::chrono::steady_clock::now();
                                    std::cout << "Time taken for get_page_buf: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                                    if (page_buffer != nullptr)
                                    {
                                        begin = std::chrono::steady_clock::now();
                                        uint64_t page_offset_tmp = PageAllocator::memory_pool->get_offset(page_buffer);
                                        end = std::chrono::steady_clock::now();
                                        std::cout << "Time taken for get_offset: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;

                                        begin = std::chrono::steady_clock::now();
                                        page_map.add_to_map(block_id, page_offset_tmp);
                                        end = std::chrono::steady_clock::now();
                                        std::cout << "Time taken for add_to_map: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " us" << std::endl;
                                    }
                                }
                                else
                                {
                                    page_map.add_to_map(block_id, static_cast<size_t>(-1));
                                }
                            }
                            return page;
                        }
                        else
                        {
                            std::cerr << "Error: Block data unavailable for block_id " << block_id << std::endl;
                        }
                    }
                    else
                    {
                        //     // current_page_t *page = new current_page_t(block_id);
                        page_it = current_pages_.insert(
                            page_it, std::make_pair(block_id, new current_page_t(block_id)));
                        page_t *page_instance = current_pages_[block_id]->page_.get_page_for_read();

                        if (page_instance != nullptr)
                        {
                            void *page_buffer = page_instance->get_page_buf(this);

                            if (page_buffer != nullptr)
                            {
                                uint64_t page_offset_tmp = PageAllocator::memory_pool->get_offset(page_buffer);
                                page_map.add_to_map(block_id, page_offset_tmp);
                            }
                            else
                            {
                                std::cerr << "Error: Buffer data unavailable for block_id " << block_id << std::endl;
                            }
                        }
                        else
                        {
                            page_map.add_to_map(block_id, static_cast<size_t>(-1));
                        }
                        misses_++;
                    }
                }

                if (isRead && RDMA_ENABLED && !PRINT_LATENCY)
                {
                    page_it = RDMA_current_pages_.find(block_id);
                    if (page_it == RDMA_current_pages_.end())
                    {
                        // std::cout << "RDMA ENABLED and PRINT_LATENCY is false" << std::endl;
                        std::pair<RDMAClient *, size_t> tmp;
                        RDMAClient *client = nullptr;
                        size_t offset = 0;

                        if (page_map.port_number == 6001)
                        {
                            tmp = PageAllocator::memory_pool->check_block_exists(block_id);
                            client = tmp.first;
                            offset = tmp.second;
                        }
                        if (client != nullptr && block_id != 0 && offset != static_cast<size_t>(-1))
                        {
                            auto begin = std::chrono::steady_clock::now();
                            uint32_t page_size = max_block_size_.value();
                            // std::cout << "Block " << block_id << " exists on remote server." << client->getIP() << std::endl;
                            void *block_data = client->getPageFromOffset(offset, page_size);
                            auto end = std::chrono::steady_clock::now();
                            RDMA_latency.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());

                            if (block_data != nullptr)
                            {
                                RDMA_hits_.fetch_add(1);
                                uint32_t ser_bs = page_size + sizeof(ls_buf_data_t); // Total serialized block size
                                block_size_t block_size = block_size_t::unsafe_make(ser_bs);

                                buf_ptr_t buf = buf_ptr_t::alloc_uninitialized(block_size);
                                std::memcpy(buf.cache_data(), block_data, page_size);

                                buf.fill_padding_zero();
                                current_page_t *page = new current_page_t(block_id, std::move(buf), this, true);
                                client->addFrequencyMapEntry(block_id);
                                bool internal_page = check_if_internal_page(block_data);

                                // if (check_if_node_in_range(block_id) || internal_page)
                                // if (client->performFrequencyMapLookup(block_id) || internal_page || check_if_key_can_be_admitted(block_id))
                                // if (check_if_node_in_range(block_id) || internal_page || check_if_key_can_be_admitted(block_id))
                                if (check_if_node_in_range(block_id) || internal_page || check_if_key_can_be_admitted(block_id))
                                {
                                    page_it = RDMA_current_pages_.insert(page_it, std::make_pair(block_id, page));

                                    page_t *page_instance = RDMA_current_pages_[block_id]->page_.get_page_for_read();

                                    update_cache_page(page_instance, block_id);

                                    if (internal_page)
                                    {
                                        update_leaf_map(block_id, true);
                                    }
                                }
                                update_block_info_map(block_id, internal_page, false, false, true);
                                return page;
                            }
                            else
                            {
                                std::cerr << "Error: Block data unavailable for block_id " << block_id << std::endl;
                            }
                        }
                        else
                        {
                            auto tmp = new current_page_t(block_id);
                            // if (check_if_node_in_range(block_id) || check_leaf_map_if_leaf(block_id))
                            // if (!check_if_block_duplicate(block_id) || check_leaf_map_if_leaf(block_id))
                            // if (!check_if_block_duplicate(block_id) || check_leaf_map_if_leaf(block_id) || check_if_key_can_be_admitted(block_id))
                            // if (true)
                            if (check_if_node_in_range(block_id) || check_leaf_map_if_leaf(block_id) || check_if_key_can_be_admitted(block_id))
                            {
                                // page_it = current_pages_.insert(
                                //     page_it, std::make_pair(block_id, tmp));
                                page_it = RDMA_current_pages_.insert(
                                    page_it, std::make_pair(block_id, tmp));
                                page_t *page_instance = RDMA_current_pages_[block_id]->page_.get_page_for_read();
                                update_cache_page(page_instance, block_id);
                            }
                            // else
                            // {
                            // if (check_leaf_map_if_leaf(block_id))
                            // {
                            //     std::cout << "Block " << block_id << " is an internal node" << std::endl;
                            // }
                            // std::cout << "Block " << block_id << " already exists in the cache." << std::endl;
                            // }
                            misses_++;
                            update_block_info_map(block_id, false, false, true, false);
                            return tmp;
                        }
                    }
                }
                else
                {
                    // current_page_t *page = new current_page_t(block_id);
                    page_it = current_pages_.insert(
                        page_it, std::make_pair(block_id, new current_page_t(block_id)));
                    // page_it = RDMA_current_pages_.insert(
                    //     page_it, std::make_pair(block_id, new current_page_t(block_id)));
                    page_t *page_instance = page_it->second->page_.get_page_for_read();
                    update_cache_page(page_instance, block_id);
                    misses_++;
                    update_block_info_map(block_id, false, false, true, false);
                }
            }
            else
            {
                update_perf_map(block_id);
                page_t *page_instance = page_it->second->page_.get_page_for_read();
                if (page_instance->is_loaded())
                {
                    if (check_if_internal_page(page_instance))
                    {
                        update_leaf_map(block_id, true);
                        update_block_info_map(block_id, true, false, false, false);
                    }
                }
                update_block_info_map(block_id, false, true, false, false);
                rassert(!page_it->second->is_deleted());
            }
        }

        if (PRINT_RDMA_MISSRATE)
        {
            if (misses_ > 77700 && !clean_up_after_writes)
            {
                if (RDMA_ENABLED)
                {
                    evicter_.remove_non_leaf_before_read();
                }
                clean_up_after_writes = true;
            }
            operation_count.fetch_add(1);
            if (operation_count.load() % 1000000 == 0)
            {
                latency_info_.RDMA = avg_rdma_latency();
                if (clean_up_after_writes)
                {
                    if (max_block_size_.value() == 0)
                    {
                        throw std::runtime_error("max_block_size_ cannot be zero");
                    }
                    uint64_t cache_size_in_blocks = evicter_.memory_limit() / max_block_size_.value();
                    if (cache_size_in_blocks == 0)
                    {
                        throw std::runtime_error("cache_size_in_blocks cannot be zero");
                    }
                    // get_and_sort_freq(perf_map, cdf_result);
                    get_best_access_rates(perf_map, cdf_result, latency_info_.cache, latency_info_.disk, latency_info_.RDMA, cache_size_in_blocks, keys_that_can_be_admitted);
                    clear_perf_map();
                }
                // evicter_.remove_out_of_range_pages_periodically();
                // client->cleanupFrequencyMap();
                std::cout
                    << "RDMA bags: " << rdma_bag << " Unevictable bags: "
                    << unevictable_bag << " Evicted bags: " << evicted_bag
                    << " Evictable disk backed bags: " << evictable_disk_backed_bag
                    << " Evictable unbacked bags: " << evictable_unbacked_bag << std::endl;
                std::cout << "RDMA Latency: " << latency_info_.RDMA << " total admitted" << total_admitted << "writes hits " << writes_hit << std::endl;
                // std::cout << "load_with_block_id_: " << load_with_block_id_ << " load_using_block_token_: "
                //           << load_using_block_token_ << " finish_load_with_block_id_: "
                //           << finish_load_with_block_id_ << " catch_up_with_deferred_load_: "
                //           << catch_up_with_deferred_load_ << " is_pages_not_in_cache_: "
                //           << is_pages_not_in_cache_ << std::endl;
                // evicter_.print_all_bag_sizes();
                std::cout << "RDMA hits: " << RDMA_hits_.load() << " Miss rate: " << misses_ << std::endl;
            }
        }
        if (PRINT_MAPS)
        {
            // if (!PRINT_RDMA_MISSRATE)
            // {
            //     operation_count.fetch_add(1);
            // }
            {
                std::lock_guard<std::mutex> lock(file_number_mutex);
                if (operation_count.load() >= 1000000)
                {
                    // std::cout << "RDMA hits: " << RDMA_hits_.load() << " Miss rate: " << misses_ << std::endl;
                    print_current_pages_to_file(page_map.file_number);
                    // print_block_info_map(page_map.file_number);
                    // print_leaf_map(page_map.file_number);
                    // print_cdf(cdf_result, page_map.file_number);
                    // print_keys_that_can_be_admitted(page_map.file_number);
                    // print_perf_map(page_map.file_number);
                    // page_map.print_map_to_file(page_map.file_number);
                    // page_map.file_number++;
                    operation_count.store(0);
                    // for (RDMAClient *client : PageAllocator::memory_pool->RemoteMetadata)
                    // {
                    //     client->getPageMap()->print_map_to_file_remote_metadata(client->getIP(), client->getPageMap()->file_number++);
                    // }
                }
            }
        }
        if (write_key_found)
        {
            return write_page_it->second;
        }
        else
        {
            return page_it->second;
        }
    }

    current_page_t *page_cache_t::page_for_new_block_id(
        block_type_t block_type,
        block_id_t *block_id_out)
    {
        assert_thread();
        block_id_t block_id;
        switch (block_type)
        {
        case block_type_t::aux:
            block_id = free_list_.acquire_aux_block_id();
            break;
        case block_type_t::normal:
            block_id = free_list_.acquire_block_id();
            break;
        default:
            unreachable();
        }
        current_page_t *ret = internal_page_for_new_chosen(block_id);
        *block_id_out = block_id;
        return ret;
    }

    current_page_t *page_cache_t::page_for_new_chosen_block_id(block_id_t block_id)
    {
        assert_thread();
        // Tell the free list this block id is taken.
        free_list_.acquire_chosen_block_id(block_id);
        return internal_page_for_new_chosen(block_id);
    }

    current_page_t *page_cache_t::internal_page_for_new_chosen(block_id_t block_id)
    {
        assert_thread();
        rassert(is_aux_block_id(block_id) ||
                    recency_for_block_id(block_id) == repli_timestamp_t::invalid,
                "expected chosen block %" PR_BLOCK_ID "to be deleted", block_id);
        if (!is_aux_block_id(block_id))
        {
            set_recency_for_block_id(block_id, repli_timestamp_t::distant_past);
        }

        buf_ptr_t buf = buf_ptr_t::alloc_uninitialized(max_block_size_);

#if !defined(NDEBUG) || defined(VALGRIND)
        // KSI: This should actually _not_ exist -- we are ignoring legitimate errors
        // where we write uninitialized data to disk.
        memset(buf.cache_data(), 0xCD, max_block_size_.value());
#endif
        auto *pages = &current_pages_;

        if (WRITES_ENABLED && block_id > 3)
        {
            // consider_evicting_all_write_pages(this);

            auto post_cp = current_pages_.size();
            auto post_wcp = write_current_pages_.size();
            // printf("POST SIZE %d %d\n", post_cp, post_wcp);
            pages = &write_current_pages_;
        }

        auto inserted_page = pages->insert(std::make_pair(
            block_id, new current_page_t(block_id, std::move(buf), this)));
        guarantee(inserted_page.second);

        page_t *page_instance = inserted_page.first->second->page_.get_page_for_read();

        if (page_instance != nullptr)
        {
            page_instance->is_write = true;
            void *page_buffer = page_instance->get_page_buf(this);

            if (page_buffer != nullptr)
            {
                uint64_t page_offset_tmp = PageAllocator::memory_pool->get_offset(page_buffer);
                page_map.add_to_map(block_id, page_offset_tmp);
            }
            else
            {
                std::cerr << "Error: Buffer data unavailable for block_id " << block_id << std::endl;
            }
        }
        else
        {
            page_map.add_to_map(block_id, static_cast<size_t>(-1));
        }

        misses_++;

        return inserted_page.first->second;
    }
    void page_cache_t::erase_write_page_for_block_id(block_id_t block_id)
    {
        auto page_it = write_current_pages_.find(block_id);
        if (page_it == write_current_pages_.end())
        {
            return;
        }
        current_page_t *page_ptr = page_it->second;
        if (page_ptr->should_be_evicted())
        {
            write_current_pages_.erase(block_id);
            page_ptr->reset(this);
            delete page_ptr;
        }
    }

    void page_cache_t::consider_evicting_all_write_pages(page_cache_t *page_cache)
    {
        // Atomically grab a list of block IDs that currently exist in current_pages.
        std::vector<block_id_t> current_block_ids;
        current_block_ids.reserve(page_cache->write_current_pages_.size());
        for (const auto &current_page : page_cache->write_current_pages_)
        {
            current_block_ids.push_back(current_page.first);
        }
        // In a separate step, evict current pages that should be evicted.
        // We do this separately so that we can yield between evictions.
        size_t i = 0;
        for (block_id_t id : current_block_ids)
        {
            page_cache->erase_write_page_for_block_id(id);
            ++i;
        }
    }

    cache_account_t page_cache_t::create_cache_account(int priority)
    {
        // We assume that a priority of 100 means that the transaction should have the
        // same priority as all the non-accounted transactions together. Not sure if this
        // makes sense.

        // Be aware of rounding errors... (what can be do against those? probably just
        // setting the default io_priority_reads high enough)
        int io_priority = std::max(1, CACHE_READS_IO_PRIORITY * priority / 100);

        // TODO: This is a heuristic. While it might not be evil, it's not really optimal
        // either.
        int outstanding_requests_limit = std::max(1, 16 * priority / 100);

        file_account_t *io_account;
        {
            // Ideally we shouldn't have to switch to the serializer thread.  But that's
            // what the file account API is right now, deep in the I/O layer.
            on_thread_t thread_switcher(serializer_->home_thread());
            io_account = serializer_->make_io_account(io_priority,
                                                      outstanding_requests_limit);
        }

        return cache_account_t(serializer_->home_thread(), io_account);
    }

    current_page_acq_t::current_page_acq_t()
        : page_cache_(nullptr), the_txn_(nullptr) {}

    current_page_acq_t::current_page_acq_t(page_txn_t *txn,
                                           block_id_t _block_id,
                                           access_t _access,
                                           page_create_t create)
        : page_cache_(nullptr), the_txn_(nullptr)
    {
        init(txn, _block_id, _access, create);
    }

    current_page_acq_t::current_page_acq_t(page_txn_t *txn,
                                           alt_create_t create,
                                           block_type_t block_type)
        : page_cache_(nullptr), the_txn_(nullptr)
    {
        init(txn, create, block_type);
    }

    current_page_acq_t::current_page_acq_t(page_cache_t *_page_cache,
                                           block_id_t _block_id,
                                           read_access_t read)
        : page_cache_(nullptr), the_txn_(nullptr)
    {
        init(_page_cache, _block_id, read);
    }

    void current_page_acq_t::init(page_txn_t *txn,
                                  block_id_t _block_id,
                                  access_t _access,
                                  page_create_t create)
    {
        if (_access == access_t::read)
        {
            rassert(create == page_create_t::no);
            init(txn->page_cache(), _block_id, read_access_t::read);
        }
        else
        {
            txn->page_cache()->assert_thread();
            guarantee(page_cache_ == nullptr);
            page_cache_ = txn->page_cache();
            the_txn_ = (_access == access_t::write ? txn : nullptr);
            access_ = _access;
            declared_snapshotted_ = false;
            block_id_ = _block_id;
            if (create == page_create_t::yes)
            {
                current_page_ = page_cache_->page_for_new_chosen_block_id(_block_id);
            }
            else
            {
                current_page_ = page_cache_->page_for_block_id(_block_id, false);
            }
            dirtied_page_ = false;
            touched_page_ = false;

            the_txn_->add_acquirer(this);
            current_page_->add_acquirer(this);
        }
    }

    void current_page_acq_t::init(page_txn_t *txn,
                                  alt_create_t,
                                  block_type_t block_type)
    {
        txn->page_cache()->assert_thread();
        guarantee(page_cache_ == nullptr);
        page_cache_ = txn->page_cache();
        the_txn_ = txn;
        access_ = access_t::write;
        declared_snapshotted_ = false;
        current_page_ = page_cache_->page_for_new_block_id(block_type, &block_id_);
        dirtied_page_ = false;
        touched_page_ = false;

        the_txn_->add_acquirer(this);
        current_page_->add_acquirer(this);
    }

    void current_page_acq_t::init(page_cache_t *_page_cache,
                                  block_id_t _block_id,
                                  read_access_t)
    {
        _page_cache->assert_thread();
        guarantee(page_cache_ == nullptr);
        page_cache_ = _page_cache;
        the_txn_ = nullptr;
        access_ = access_t::read;
        declared_snapshotted_ = false;
        block_id_ = _block_id;
        current_page_ = page_cache_->page_for_block_id(_block_id, true);
        dirtied_page_ = false;
        touched_page_ = false;

        current_page_->add_acquirer(this);
    }

    current_page_acq_t::~current_page_acq_t()
    {
        assert_thread();
        // Checking page_cache_ != nullptr makes sure this isn't a default-constructed acq.
        if (page_cache_ != nullptr)
        {
            if (the_txn_ != nullptr)
            {
                guarantee(access_ == access_t::write);
                the_txn_->remove_acquirer(this);
            }
            rassert(current_page_ != nullptr);
            if (in_a_list())
            {
                // Note that the current_page_acq can be in the current_page_ acquirer
                // list and still be snapshotted. However it will not have a
                // snapshotted_page_.
                rassert(!snapshotted_page_.has());
                current_page_->remove_acquirer(this);
            }
            if (declared_snapshotted_)
            {
                snapshotted_page_.reset_page_ptr(page_cache_);
                current_page_->remove_keepalive();
            }
            page_cache_->consider_evicting_current_page(block_id_);
        }
    }

    void current_page_acq_t::declare_readonly()
    {
        assert_thread();
        access_ = access_t::read;
        if (current_page_ != nullptr)
        {
            current_page_->pulse_pulsables(this);
        }
    }

    void current_page_acq_t::declare_snapshotted()
    {
        assert_thread();
        rassert(access_ == access_t::read);

        // Allow redeclaration of snapshottedness.
        if (!declared_snapshotted_)
        {
            declared_snapshotted_ = true;
            rassert(current_page_ != nullptr);
            current_page_->add_keepalive();
            current_page_->pulse_pulsables(this);
        }
    }

    signal_t *current_page_acq_t::read_acq_signal()
    {
        assert_thread();
        return &read_cond_;
    }

    signal_t *current_page_acq_t::write_acq_signal()
    {
        assert_thread();
        rassert(access_ == access_t::write);
        return &write_cond_;
    }

    page_t *current_page_acq_t::current_page_for_read(cache_account_t *account)
    {
        assert_thread();
        rassert(snapshotted_page_.has() || current_page_ != nullptr);
        read_cond_.wait();
        if (snapshotted_page_.has())
        {
            return snapshotted_page_.get_page_for_read();
        }
        rassert(current_page_ != nullptr);
        return current_page_->the_page_for_read(help(), account);
    }

    repli_timestamp_t current_page_acq_t::recency()
    {
        assert_thread();
        rassert(snapshotted_page_.has() || current_page_ != nullptr);

        // We wait for write_cond_ when getting the recency (if we're a write acquirer)
        // so that we can't see the recency change before/after the write_cond_ is
        // pulsed.
        if (access_ == access_t::read)
        {
            read_cond_.wait();
        }
        else
        {
            write_cond_.wait();
        }

        if (snapshotted_page_.has())
        {
            return snapshotted_page_.timestamp();
        }
        rassert(current_page_ != nullptr);
        return page_cache_->recency_for_block_id(block_id_);
    }

    page_t *current_page_acq_t::current_page_for_write(cache_account_t *account)
    {
        assert_thread();
        rassert(access_ == access_t::write);
        rassert(current_page_ != nullptr);
        write_cond_.wait();
        rassert(current_page_ != nullptr);
        dirtied_page_ = true;
        return current_page_->the_page_for_write(help(), account);
    }

    void current_page_acq_t::set_recency(repli_timestamp_t _recency)
    {
        assert_thread();
        rassert(access_ == access_t::write);
        rassert(current_page_ != nullptr);
        write_cond_.wait();
        rassert(current_page_ != nullptr);
        touched_page_ = true;
        page_cache_->set_recency_for_block_id(block_id_, _recency);
    }

    void current_page_acq_t::mark_deleted()
    {
        assert_thread();
        rassert(access_ == access_t::write);
        rassert(current_page_ != nullptr);
        write_cond_.wait();
        rassert(current_page_ != nullptr);
        dirtied_page_ = true;
        current_page_->mark_deleted(help());
        // No need to call consider_evicting_current_page here -- there's a
        // current_page_acq_t for it: ourselves.
    }

    bool current_page_acq_t::dirtied_page() const
    {
        assert_thread();
        return dirtied_page_;
    }

    bool current_page_acq_t::touched_page() const
    {
        assert_thread();
        return touched_page_;
    }

    block_version_t current_page_acq_t::block_version() const
    {
        assert_thread();
        return block_version_;
    }

    page_cache_t *current_page_acq_t::page_cache() const
    {
        assert_thread();
        return page_cache_;
    }

    current_page_help_t current_page_acq_t::help() const
    {
        assert_thread();
        return current_page_help_t(block_id(), page_cache_);
    }

    void current_page_acq_t::pulse_read_available()
    {
        assert_thread();
        read_cond_.pulse_if_not_already_pulsed();
    }

    void current_page_acq_t::pulse_write_available()
    {
        assert_thread();
        write_cond_.pulse_if_not_already_pulsed();
    }

    current_page_t::current_page_t(block_id_t block_id)
        : block_id_(block_id),
          is_deleted_(false),
          last_write_acquirer_(nullptr),
          num_keepalives_(0)
    {
        // Increment the block version so that we can distinguish between unassigned
        // current_page_acq_t::block_version_ values (which are 0) and assigned ones.
        rassert(last_write_acquirer_version_.debug_value() == 0);
        last_write_acquirer_version_ = last_write_acquirer_version_.subsequent();
    }

    current_page_t::current_page_t(block_id_t block_id,
                                   buf_ptr_t buf,
                                   page_cache_t *page_cache)
        : block_id_(block_id),
          page_(new page_t(block_id, std::move(buf), page_cache)),
          is_deleted_(false),
          last_write_acquirer_(nullptr),
          num_keepalives_(0)
    {
        // Increment the block version so that we can distinguish between unassigned
        // current_page_acq_t::block_version_ values (which are 0) and assigned ones.
        rassert(last_write_acquirer_version_.debug_value() == 0);
        last_write_acquirer_version_ = last_write_acquirer_version_.subsequent();
    }

    current_page_t::current_page_t(block_id_t block_id,
                                   buf_ptr_t buf,
                                   page_cache_t *page_cache,
                                   bool isRDMA)
        : block_id_(block_id),
          page_(new page_t(block_id, std::move(buf), page_cache, isRDMA)),
          is_deleted_(false),
          last_write_acquirer_(nullptr),
          num_keepalives_(0)
    {
        // Increment the block version so that we can distinguish between unassigned
        // current_page_acq_t::block_version_ values (which are 0) and assigned ones.
        rassert(last_write_acquirer_version_.debug_value() == 0);
        last_write_acquirer_version_ = last_write_acquirer_version_.subsequent();
    }

    current_page_t::current_page_t(block_id_t block_id,
                                   buf_ptr_t buf,
                                   const counted_t<standard_block_token_t> &token,
                                   page_cache_t *page_cache)
        : block_id_(block_id),
          page_(new page_t(block_id, std::move(buf), token, page_cache)),
          is_deleted_(false),
          last_write_acquirer_(nullptr),
          num_keepalives_(0)
    {
        // Increment the block version so that we can distinguish between unassigned
        // current_page_acq_t::block_version_ values (which are 0) and assigned ones.
        rassert(last_write_acquirer_version_.debug_value() == 0);
        last_write_acquirer_version_ = last_write_acquirer_version_.subsequent();
    }

    current_page_t::~current_page_t()
    {
        // Check that reset() has been called.
        rassert(last_write_acquirer_version_.debug_value() == 0);

        // An imperfect sanity check.
        rassert(!page_.has());
        rassert(num_keepalives_ == 0);
    }

    bool current_page_t::is_rdma_page()
    {
        return this->the_page_for_read_for_RDMA()->is_rdma_page();
    }

    void current_page_t::reset(page_cache_t *page_cache)
    {
        rassert(acquirers_.empty());
        rassert(num_keepalives_ == 0);

        // KSI: Does last_write_acquirer_ even need to be NULL?  Could we not just inform
        // it of our impending destruction?
        rassert(last_write_acquirer_ == nullptr);

        page_.reset_page_ptr(page_cache);
        // No need to call consider_evicting_current_page here -- we're already getting
        // destructed.

        // For the sake of the ~current_page_t assertion.
        last_write_acquirer_version_ = block_version_t();

        if (is_deleted_ && block_id_ != NULL_BLOCK_ID)
        {
            page_cache->free_list()->release_block_id(block_id_);
            block_id_ = NULL_BLOCK_ID;
        }
    }

    bool current_page_t::should_be_evicted() const
    {
        // Consider reasons why the current_page_t should not be evicted.

        // A reason: It still has acquirers.  (Important.)
        if (!acquirers_.empty())
        {
            return false;
        }

        // A reason: We still have a connection to last_write_acquirer_.  (Important.)
        if (last_write_acquirer_ != nullptr)
        {
            return false;
        }

        // A reason: The current_page_t is kept alive for another reason.  (Important.)
        if (num_keepalives_ > 0)
        {
            return false;
        }

        // A reason: Its page_t isn't evicted, or has other snapshotters or waiters
        // anyway.  (Getting this wrong can only hurt performance.  We want to evict
        // current_page_t's with unloaded, otherwise unused page_t's.)
        if (page_.has())
        {
            page_t *page = page_.get_page_for_read();
            if (page->is_loading() || page->has_waiters() || page->is_loaded() || page->page_ptr_count() != 1)
            {
                return false;
            }
            // is_loading is false and is_loaded is false -- it must be disk-backed.
            rassert(page->is_disk_backed() || page->is_deferred_loading());
        }

        return true;
    }

    void current_page_t::add_acquirer(current_page_acq_t *acq)
    {
        const block_version_t prev_version = last_write_acquirer_version_;

        if (acq->access_ == access_t::write)
        {
            block_version_t v = prev_version.subsequent();
            acq->block_version_ = v;

            rassert(acq->the_txn_ != nullptr);
            page_txn_t *const acq_txn = acq->the_txn_;

            last_write_acquirer_version_ = v;

            if (last_write_acquirer_ != acq_txn)
            {
                rassert(!acq_txn->pages_write_acquired_last_.has_element(this));

                if (last_write_acquirer_ != nullptr)
                {
                    page_txn_t *prec = last_write_acquirer_;

                    rassert(prec->pages_write_acquired_last_.has_element(this));
                    prec->pages_write_acquired_last_.remove(this);

                    acq_txn->connect_preceder(prec);
                }

                acq_txn->pages_write_acquired_last_.add(this);
                last_write_acquirer_ = acq_txn;
            }
        }
        else
        {
            rassert(acq->the_txn_ == nullptr);
            acq->block_version_ = prev_version;
        }

        acquirers_.push_back(acq);
        pulse_pulsables(acq);
    }

    void current_page_t::remove_acquirer(current_page_acq_t *acq)
    {
        current_page_acq_t *next = acquirers_.next(acq);
        acquirers_.remove(acq);
        if (next != nullptr)
        {
            pulse_pulsables(next);
        }
    }

    void current_page_t::pulse_pulsables(current_page_acq_t *const acq)
    {
        const current_page_help_t help = acq->help();

        // First, avoid pulsing when there's nothing to pulse.
        {
            current_page_acq_t *prev = acquirers_.prev(acq);
            if (!(prev == nullptr || (prev->access_ == access_t::read && prev->read_cond_.is_pulsed())))
            {
                return;
            }
        }

        // Second, avoid re-pulsing already-pulsed chains.
        if (acq->access_ == access_t::read && acq->read_cond_.is_pulsed() && !acq->declared_snapshotted_)
        {
            // acq was pulsed for read, but it could have been a write acq at that time,
            // so the next node might not have been pulsed for read.  Also we might as
            // well stop if we're at the end of the chain (and have been pulsed).
            current_page_acq_t *next = acquirers_.next(acq);
            if (next == nullptr || next->read_cond_.is_pulsed())
            {
                return;
            }
        }

        const repli_timestamp_t current_recency = help.page_cache->recency_for_block_id(help.block_id);

        // It's time to pulse the pulsables.
        current_page_acq_t *cur = acq;
        while (cur != nullptr)
        {
            // We know that the previous node has read access and has been pulsed as
            // readable, so we pulse the current node as readable.
            cur->pulse_read_available();

            if (cur->access_ == access_t::read)
            {
                current_page_acq_t *next = acquirers_.next(cur);
                if (cur->declared_snapshotted_)
                {
                    // Snapshotters get kicked out of the queue, to make way for
                    // write-acquirers.

                    // We treat deleted pages this way because a write-acquirer may
                    // downgrade itself to readonly and snapshotted for the sake of
                    // flushing its version of the page -- and if it deleted the page,
                    // this is how it learns.

                    cur->snapshotted_page_.init(
                        current_recency,
                        the_page_for_read_or_deleted(help));
                    acquirers_.remove(cur);
                }
                cur = next;
            }
            else
            {
                // Even the first write-acquirer gets read access (there's no need for an
                // "intent" mode).  But subsequent acquirers need to wait, because the
                // write-acquirer might modify the value.
                if (acquirers_.prev(cur) == nullptr)
                {
                    // (It gets exclusive write access if there's no preceding reader.)
                    guarantee(!is_deleted_);
                    cur->pulse_write_available();
                }
                break;
            }
        }
    }

    void current_page_t::add_keepalive()
    {
        ++num_keepalives_;
    }

    void current_page_t::remove_keepalive()
    {
        guarantee(num_keepalives_ > 0);
        --num_keepalives_;
    }

    void current_page_t::mark_deleted(current_page_help_t help)
    {
        rassert(!is_deleted_);
        is_deleted_ = true;

        // Only the last acquirer (the current write-acquirer) of a block may mark it
        // deleted, because subsequent acquirers should not be trying to create a block
        // whose block id hasn't been released to the free list yet.
        rassert(acquirers_.size() == 1);

        help.page_cache->set_recency_for_block_id(help.block_id,
                                                  repli_timestamp_t::invalid);
        page_.reset_page_ptr(help.page_cache);
        // It's the caller's responsibility to call consider_evicting_current_page after
        // we return, if that would make sense (it wouldn't though).
    }

    void current_page_t::convert_from_serializer_if_necessary(current_page_help_t help,
                                                              cache_account_t *account)
    {
        rassert(!is_deleted_);
        if (!page_.has())
        {
            page_.init(new page_t(help.block_id, help.page_cache, account));
        }
    }

    void current_page_t::convert_from_serializer_if_necessary(current_page_help_t help)
    {
        rassert(!is_deleted_);
        if (!page_.has())
        {
            page_.init(new page_t(help.block_id, help.page_cache));
        }
    }

    page_t *current_page_t::the_page_for_read(current_page_help_t help,
                                              cache_account_t *account)
    {
        guarantee(!is_deleted_);
        convert_from_serializer_if_necessary(help, account);
        return page_.get_page_for_read();
    }

    page_t *current_page_t::the_page_for_read_for_RDMA()
    {
        guarantee(!is_deleted_);
        return page_.get_page_for_read();
    }

    page_t *current_page_t::the_page_for_read_or_deleted(current_page_help_t help)
    {
        if (is_deleted_)
        {
            return nullptr;
        }
        else
        {
            convert_from_serializer_if_necessary(help);
            return page_.get_page_for_read();
        }
    }

    page_t *current_page_t::the_page_for_write(current_page_help_t help,
                                               cache_account_t *account)
    {
        guarantee(!is_deleted_);
        convert_from_serializer_if_necessary(help, account);
        return page_.get_page_for_write(help.page_cache, account);
    }

    page_txn_t::page_txn_t(page_cache_t *_page_cache,
                           throttler_acq_t throttler_acq,
                           cache_conn_t *cache_conn)
        : page_cache_(_page_cache),
          cache_conn_(cache_conn),
          throttler_acq_(std::move(throttler_acq)),
          live_acqs_(0),
          began_waiting_for_flush_(false),
          spawned_flush_(false),
          mark_(marked_not)
    {
        if (cache_conn != nullptr)
        {
            page_txn_t *old_newest_txn = cache_conn->newest_txn_;
            cache_conn->newest_txn_ = this;
            if (old_newest_txn != nullptr)
            {
                rassert(old_newest_txn->cache_conn_ == cache_conn);
                old_newest_txn->cache_conn_ = nullptr;
                connect_preceder(old_newest_txn);
            }
        }
    }

    void page_txn_t::connect_preceder(page_txn_t *preceder)
    {
        page_cache_->assert_thread();
        rassert(preceder->page_cache_ == page_cache_);
        // We can't add ourselves as a preceder, we have to avoid that.
        rassert(preceder != this);
        // The flush_complete_cond_ is pulsed at the same time that this txn is removed
        // entirely from the txn graph, so we can't be adding preceders after that point.
        rassert(!preceder->flush_complete_cond_.is_pulsed());

        // See "PERFORMANCE(preceders_)".
        if (std::find(preceders_.begin(), preceders_.end(), preceder) == preceders_.end())
        {
            preceders_.push_back(preceder);
            preceder->subseqers_.push_back(this);
        }
    }

    void page_txn_t::remove_preceder(page_txn_t *preceder)
    {
        // See "PERFORMANCE(preceders_)".
        auto it = std::find(preceders_.begin(), preceders_.end(), preceder);
        rassert(it != preceders_.end());
        preceders_.erase(it);
    }

    void page_txn_t::remove_subseqer(page_txn_t *subseqer)
    {
        // See "PERFORMANCE(subseqers_)".
        auto it = std::find(subseqers_.begin(), subseqers_.end(), subseqer);
        rassert(it != subseqers_.end());
        subseqers_.erase(it);
    }

    page_txn_t::~page_txn_t()
    {
        guarantee(flush_complete_cond_.is_pulsed());

        guarantee(preceders_.empty());
        guarantee(subseqers_.empty());

        guarantee(snapshotted_dirtied_pages_.empty());
    }

    void page_txn_t::add_acquirer(DEBUG_VAR current_page_acq_t *acq)
    {
        rassert(acq->access_ == access_t::write);
        ++live_acqs_;
    }

    void page_txn_t::remove_acquirer(current_page_acq_t *acq)
    {
        guarantee(acq->access_ == access_t::write);
        // This is called by acq's destructor.
        {
            rassert(live_acqs_ > 0);
            --live_acqs_;
        }

        // It's not snapshotted because you can't snapshot write acqs.  (We
        // rely on this fact solely because we need to grab the block_id_t
        // and current_page_acq_t currently doesn't know it.)
        rassert(acq->current_page_ != nullptr);

        const block_version_t block_version = acq->block_version();

        if (acq->dirtied_page())
        {
            // We know we hold an exclusive lock.
            rassert(acq->write_cond_.is_pulsed());

            // Declare readonly (so that we may declare acq snapshotted).
            acq->declare_readonly();
            acq->declare_snapshotted();

            // Steal the snapshotted page_ptr_t.
            timestamped_page_ptr_t local = std::move(acq->snapshotted_page_);
            // It's okay to have two dirtied_page_t's or touched_page_t's for the
            // same block id -- compute_changes handles this.
            snapshotted_dirtied_pages_.push_back(dirtied_page_t(block_version,
                                                                acq->block_id(),
                                                                std::move(local)));
            // If you keep writing and reacquiring the same page, though, the count
            // might be off and you could excessively throttle new operations.

            // LSI: We could reacquire the same block and update the dirty page count
            // with a _correct_ value indicating that we're holding redundant dirty
            // pages for the same block id.
            throttler_acq_.update_dirty_page_count(snapshotted_dirtied_pages_.size());
        }
        else if (acq->touched_page())
        {
            // It's okay to have two dirtied_page_t's or touched_page_t's for the
            // same block id -- compute_changes handles this.
            touched_pages_.push_back(touched_page_t(block_version, acq->block_id(),
                                                    acq->recency()));
        }
    }

    void page_txn_t::announce_waiting_for_flush()
    {
        rassert(live_acqs_ == 0);
        rassert(!began_waiting_for_flush_);
        rassert(!spawned_flush_);
        began_waiting_for_flush_ = true;
        page_cache_->im_waiting_for_flush(this);
    }

    std::map<block_id_t, page_cache_t::block_change_t>
    page_cache_t::compute_changes(const std::vector<page_txn_t *> &txns)
    {
        // We combine changes, using the block_version_t value to see which change
        // happened later.  This even works if a single transaction acquired the same
        // block twice.

        // The map of changes we make.
        std::map<block_id_t, block_change_t> changes;

        for (auto it = txns.begin(); it != txns.end(); ++it)
        {
            page_txn_t *txn = *it;
            for (size_t i = 0, e = txn->snapshotted_dirtied_pages_.size(); i < e; ++i)
            {
                const dirtied_page_t &d = txn->snapshotted_dirtied_pages_[i];

                block_change_t change(d.block_version, true,
                                      d.ptr.has() ? d.ptr.get_page_for_read() : nullptr,
                                      d.ptr.has() ? d.ptr.timestamp() : repli_timestamp_t::invalid);

                auto res = changes.insert(std::make_pair(d.block_id, change));

                if (!res.second)
                {
                    // The insertion failed -- we need to use the newer version.
                    auto const jt = res.first;
                    // The versions can't be the same for different write operations.
                    rassert(jt->second.version != change.version,
                            "equal versions on block %" PRIi64 ": %" PRIu64,
                            d.block_id,
                            change.version.debug_value());
                    if (jt->second.version < change.version)
                    {
                        jt->second = change;
                    }
                }
            }
        }

        for (auto it = txns.begin(); it != txns.end(); ++it)
        {
            page_txn_t *txn = *it;
            for (size_t i = 0, e = txn->touched_pages_.size(); i < e; ++i)
            {
                const touched_page_t &t = txn->touched_pages_[i];

                auto res = changes.insert(std::make_pair(t.block_id,
                                                         block_change_t(t.block_version,
                                                                        false,
                                                                        nullptr,
                                                                        t.tstamp)));
                if (!res.second)
                {
                    // The insertion failed.  We need to combine the versions.
                    auto const jt = res.first;
                    // The versions can't be the same for different write operations.
                    rassert(jt->second.version != t.block_version);
                    if (jt->second.version < t.block_version)
                    {
                        rassert(t.tstamp ==
                                superceding_recency(jt->second.tstamp, t.tstamp));
                        jt->second.tstamp = t.tstamp;
                        jt->second.version = t.block_version;
                    }
                }
            }
        }

        return changes;
    }

    void page_cache_t::remove_txn_set_from_graph(page_cache_t *page_cache,
                                                 const std::vector<page_txn_t *> &txns)
    {
        page_cache->assert_thread();

        for (auto it = txns.begin(); it != txns.end(); ++it)
        {
            // We want detaching the subsequers and preceders to happen at the same time
            // that the flush_complete_cond_ is pulsed.  That way connect_preceder can
            // check if flush_complete_cond_ has been pulsed.
            ASSERT_FINITE_CORO_WAITING;
            page_txn_t *txn = *it;
            {
                for (auto jt = txn->subseqers_.begin(); jt != txn->subseqers_.end(); ++jt)
                {
                    (*jt)->remove_preceder(txn);
                }
                txn->subseqers_.clear();
            }

            // We could have preceders outside this txn set, because transactions that
            // don't make any modifications don't get flushed, and they don't wait for
            // their preceding transactions to get flushed and then removed from the
            // graph.
            for (auto jt = txn->preceders_.begin(); jt != txn->preceders_.end(); ++jt)
            {
                (*jt)->remove_subseqer(txn);
            }
            txn->preceders_.clear();

            // KSI: Maybe we could remove pages_write_acquired_last_ earlier?  Like when
            // we begin the index write (but that's on the wrong thread) or earlier?
            while (txn->pages_write_acquired_last_.size() != 0)
            {
                current_page_t *current_page = txn->pages_write_acquired_last_.access_random(0);
                rassert(current_page->last_write_acquirer_ == txn);

#ifndef NDEBUG
                // All existing acquirers should be read acquirers, since this txn _was_
                // the last write acquirer.
                for (current_page_acq_t *acq = current_page->acquirers_.head();
                     acq != nullptr;
                     acq = current_page->acquirers_.next(acq))
                {
                    rassert(acq->access() == access_t::read);
                }
#endif

                txn->pages_write_acquired_last_.remove(current_page);
                current_page->last_write_acquirer_ = nullptr;
                page_cache->consider_evicting_current_page(current_page->block_id_);
            }

            if (txn->cache_conn_ != nullptr)
            {
                rassert(txn->cache_conn_->newest_txn_ == txn);
                txn->cache_conn_->newest_txn_ = nullptr;
                txn->cache_conn_ = nullptr;
            }

            txn->flush_complete_cond_.pulse();
        }
    }

    struct block_token_tstamp_t
    {
        block_token_tstamp_t(block_id_t _block_id,
                             bool _is_deleted,
                             counted_t<standard_block_token_t> _block_token,
                             repli_timestamp_t _tstamp,
                             page_t *_page)
            : block_id(_block_id), is_deleted(_is_deleted),
              block_token(std::move(_block_token)), tstamp(_tstamp),
              page(_page) {}
        block_id_t block_id;
        bool is_deleted;
        counted_t<standard_block_token_t> block_token;
        repli_timestamp_t tstamp;
        // The page, or nullptr, if we don't know it.
        page_t *page;
    };

    struct ancillary_info_t
    {
        ancillary_info_t(repli_timestamp_t _tstamp,
                         page_t *_page)
            : tstamp(_tstamp), page(_page) {}
        repli_timestamp_t tstamp;
        page_t *page;
    };

    void page_cache_t::do_flush_changes(page_cache_t *page_cache,
                                        std::map<block_id_t, block_change_t> &&changes,
                                        const std::vector<page_txn_t *> &txns,
                                        fifo_enforcer_write_token_t index_write_token)
    {
        rassert(!changes.empty());
        std::vector<block_token_tstamp_t> blocks_by_tokens;
        blocks_by_tokens.reserve(changes.size());

        std::vector<ancillary_info_t> ancillary_infos;
        std::vector<buf_write_info_t> write_infos;
        ancillary_infos.reserve(changes.size());
        write_infos.reserve(changes.size());

        {
            ASSERT_NO_CORO_WAITING;

            for (auto it = changes.begin(); it != changes.end(); ++it)
            {
                if (it->second.modified)
                {
                    if (it->second.page == nullptr)
                    {
                        // The block is deleted.
                        blocks_by_tokens.push_back(block_token_tstamp_t(it->first,
                                                                        true,
                                                                        counted_t<standard_block_token_t>(),
                                                                        repli_timestamp_t::invalid,
                                                                        nullptr));
                    }
                    else
                    {
                        page_t *page = it->second.page;
                        if (page->block_token().has())
                        {
                            // It's already on disk, we're not going to flush it.
                            blocks_by_tokens.push_back(block_token_tstamp_t(it->first,
                                                                            false,
                                                                            page->block_token(),
                                                                            it->second.tstamp,
                                                                            page));
                        }
                        else
                        {
                            // We can't be in the process of loading a block we're going
                            // to write for which we don't have a block token.  That's
                            // because we _actually dirtied the page_.  We had to have
                            // acquired the buf, and the only way to get rid of the buf
                            // is for it to be evicted, in which case the block token
                            // would be non-empty.

                            rassert(page->is_loaded());

                            // KSI: Is there a page_acq_t for this buf we're writing?  Is it
                            // possible that we might be trying to do an unbacked eviction
                            // for this page right now?  (No, we don't do that yet.)
                            write_infos.push_back(buf_write_info_t(page->get_loaded_ser_buffer(),
                                                                   page->get_page_buf_size(),
                                                                   it->first));
                            ancillary_infos.push_back(ancillary_info_t(it->second.tstamp,
                                                                       page));
                        }
                    }
                }
                else
                {
                    // We only touched the page.
                    blocks_by_tokens.push_back(block_token_tstamp_t(it->first,
                                                                    false,
                                                                    counted_t<standard_block_token_t>(),
                                                                    it->second.tstamp,
                                                                    nullptr));
                }
            }
        }

        cond_t blocks_released_cond;
        {
            on_thread_t th(page_cache->serializer_->home_thread());

            struct : public iocallback_t, public cond_t
            {
                void on_io_complete()
                {
                    pulse();
                }
            } blocks_written_cb;

            std::vector<counted_t<standard_block_token_t>> tokens = page_cache->serializer_->block_writes(write_infos,
                                                                                                          /* disk account is overridden
                                                                                                           * by merger_serializer_t */
                                                                                                          DEFAULT_DISK_ACCOUNT,
                                                                                                          &blocks_written_cb);

            rassert(tokens.size() == write_infos.size());
            rassert(write_infos.size() == ancillary_infos.size());
            for (size_t i = 0; i < write_infos.size(); ++i)
            {
                blocks_by_tokens.push_back(block_token_tstamp_t(write_infos[i].block_id,
                                                                false,
                                                                std::move(tokens[i]),
                                                                ancillary_infos[i].tstamp,
                                                                ancillary_infos[i].page));
            }

            // KSI: Unnecessary copying between blocks_by_tokens and write_ops, inelegant
            // representation of deletion/touched blocks in blocks_by_tokens.
            std::vector<index_write_op_t> write_ops;
            write_ops.reserve(blocks_by_tokens.size());

            for (auto it = blocks_by_tokens.begin(); it != blocks_by_tokens.end();
                 ++it)
            {
                if (it->is_deleted)
                {
                    write_ops.push_back(index_write_op_t(
                        it->block_id,
                        make_optional(counted_t<standard_block_token_t>()),
                        make_optional(repli_timestamp_t::invalid)));
                }
                else if (it->block_token.has())
                {
                    write_ops.push_back(index_write_op_t(it->block_id,
                                                         make_optional(it->block_token),
                                                         make_optional(it->tstamp)));
                }
                else
                {
                    write_ops.push_back(index_write_op_t(it->block_id,
                                                         r_nullopt,
                                                         make_optional(it->tstamp)));
                }
            }

            blocks_written_cb.wait();

            fifo_enforcer_sink_t::exit_write_t exiter(&page_cache->index_write_sink_->sink,
                                                      index_write_token);
            exiter.wait();
            new_mutex_in_line_t mutex_acq(&page_cache->index_write_sink_->mutex);
            exiter.end();

            rassert(!write_ops.empty());
            mutex_acq.acq_signal()->wait();
            page_cache->serializer_->index_write(
                &mutex_acq,
                [&]()
                {
                    // Update the block tokens and free the associated snapshots once the
                    // serializer's in-memory index has been updated (we don't need to wait
                    // until the index changes have been written to disk).
                    coro_t::spawn_on_thread([&]()
                                            {
                    // Update the block tokens of the written blocks
                    for (auto &block : blocks_by_tokens) {
                        if (block.block_token.has() && block.page != nullptr) {
                            // We know page is still a valid pointer because of the
                            // page_ptr_t in snapshotted_dirtied_pages_.

                            // KSI: This assertion would fail if we try to force-evict the page
                            // simultaneously as this write.
                            rassert(!block.page->block_token().has());
                            eviction_bag_t *old_bag
                                = page_cache->evicter().correct_eviction_category(
                                    block.page);
                            block.page->init_block_token(
                                std::move(block.block_token),
                                page_cache);
                            page_cache->evicter().change_to_correct_eviction_bag(
                                old_bag,
                                block.page);
                        }
                    }

                    // Clear `changes`, since we are going to evict the pages
                    // that it has pointers to in the next step.
                    changes.clear();
                    for (auto &txn : txns) {
                        for (size_t i = 0, e = txn->snapshotted_dirtied_pages_.size();
                             i < e;
                             ++i) {
                            txn->snapshotted_dirtied_pages_[i].ptr.reset_page_ptr(
                                page_cache);
                            page_cache->consider_evicting_current_page(
                                txn->snapshotted_dirtied_pages_[i].block_id);
                        }
                        txn->snapshotted_dirtied_pages_.clear();
                        txn->throttler_acq_.mark_dirty_pages_written();
                    }
                    blocks_released_cond.pulse(); }, page_cache->home_thread());
                },
                write_ops);
        }

        // Wait until the block release coroutine has finished to we can safely
        // continue (this is important because once we return, a page transaction
        // or even the whole page cache might get destructed).
        blocks_released_cond.wait();
    }

    void page_cache_t::do_flush_txn_set(page_cache_t *page_cache,
                                        std::map<block_id_t, block_change_t> *changes_ptr,
                                        const std::vector<page_txn_t *> &txns)
    {
        // This is called with spawn_now_dangerously!  The reason is partly so that we
        // don't put a zillion coroutines on the message loop when doing a bunch of
        // reads.  The other reason is that passing changes through a std::bind without
        // copying it would be very annoying.
        page_cache->assert_thread();

        // We're going to flush these transactions.  First we need to figure out what the
        // set of changes we're actually doing is, since any transaction may have touched
        // the same blocks.

        std::map<block_id_t, block_change_t> changes = std::move(*changes_ptr);
        rassert(!changes.empty());

        fifo_enforcer_write_token_t index_write_token = page_cache->index_write_source_.enter_write();

        // Okay, yield, thank you.
        coro_t::yield();
        do_flush_changes(page_cache, std::move(changes), txns, index_write_token);

        // Flush complete.

        // KSI: Can't we remove_txn_set_from_graph before flushing?  It would make some
        // data structures smaller.
        page_cache_t::remove_txn_set_from_graph(page_cache, txns);
    }

    std::vector<page_txn_t *> page_cache_t::maximal_flushable_txn_set(page_txn_t *base)
    {
        // Returns all transactions that can presently be flushed, given the newest
        // transaction that has had began_waiting_for_flush_ set.  (We assume all
        // previous such sets of transactions had flushing begin on them.)
        //
        // page_txn_t's `mark` fields can be in the following states:
        //  - not: the page has not yet been considered for processing
        //  - blue: the page is going to be considered for processing
        //  - green: the page _has_ been considered for processing, nothing bad so far
        //  - red: the page _has_ been considered for processing, and it is unflushable.
        //
        // By the end of the function (before we construct the return value), no
        // page_txn_t's are blue, and all subseqers of red pages are either red or not
        // marked.  All flushable page_txn_t's are green.
        //
        // Here are all possible transitions of the mark.  The states blue(1) and blue(2)
        // both have a blue mark, but the latter is known to have a red parent.
        //
        // not -> blue(1)
        // blue(1) -> red
        // blue(1) -> green
        // green -> blue(2)
        // blue(2) -> red
        //
        // From this transition table you can see that every page_txn_t is processed at
        // most twice.

        ASSERT_NO_CORO_WAITING;
        // An element is marked blue iff it's in `blue`.
        std::vector<page_txn_t *> blue;
        // All elements marked red, green, or blue are in `colored` -- we unmark them and
        // construct the return vector at the end of the function.
        std::vector<page_txn_t *> colored;

        rassert(!base->spawned_flush_);
        rassert(base->began_waiting_for_flush_);
        rassert(base->mark_ == page_txn_t::marked_not);
        base->mark_ = page_txn_t::marked_blue;
        blue.push_back(base);
        colored.push_back(base);

        while (!blue.empty())
        {
            page_txn_t *txn = blue.back();
            blue.pop_back();

            rassert(!txn->spawned_flush_);
            rassert(txn->began_waiting_for_flush_);
            rassert(txn->mark_ == page_txn_t::marked_blue);

            bool poisoned = false;
            for (auto it = txn->preceders_.begin(); it != txn->preceders_.end(); ++it)
            {
                page_txn_t *prec = *it;
                if (prec->spawned_flush_)
                {
                    rassert(prec->mark_ == page_txn_t::marked_not);
                }
                else if (!prec->began_waiting_for_flush_ || prec->mark_ == page_txn_t::marked_red)
                {
                    poisoned = true;
                }
                else if (prec->mark_ == page_txn_t::marked_not)
                {
                    prec->mark_ = page_txn_t::marked_blue;
                    blue.push_back(prec);
                    colored.push_back(prec);
                }
                else
                {
                    rassert(prec->mark_ == page_txn_t::marked_green || prec->mark_ == page_txn_t::marked_blue);
                }
            }

            txn->mark_ = poisoned ? page_txn_t::marked_red : page_txn_t::marked_green;

            for (auto it = txn->subseqers_.begin(); it != txn->subseqers_.end(); ++it)
            {
                page_txn_t *subs = *it;
                rassert(!subs->spawned_flush_);
                if (!subs->began_waiting_for_flush_)
                {
                    rassert(subs->mark_ == page_txn_t::marked_not);
                }
                else if (subs->mark_ == page_txn_t::marked_not)
                {
                    if (!poisoned)
                    {
                        subs->mark_ = page_txn_t::marked_blue;
                        blue.push_back(subs);
                        colored.push_back(subs);
                    }
                }
                else if (subs->mark_ == page_txn_t::marked_green)
                {
                    if (poisoned)
                    {
                        subs->mark_ = page_txn_t::marked_blue;
                        blue.push_back(subs);
                    }
                }
                else
                {
                    rassert(subs->mark_ == page_txn_t::marked_red || subs->mark_ == page_txn_t::marked_blue);
                }
            }
        }

        auto it = colored.begin();
        auto jt = it;

        while (jt != colored.end())
        {
            page_txn_t::mark_state_t mark = (*jt)->mark_;
            (*jt)->mark_ = page_txn_t::marked_not;
            if (mark == page_txn_t::marked_green)
            {
                *it++ = *jt++;
            }
            else
            {
                rassert(mark == page_txn_t::marked_red);
                ++jt;
            }
        }

        colored.erase(it, colored.end());
        return colored;
    }

    void page_cache_t::im_waiting_for_flush(page_txn_t *base)
    {
        assert_thread();
        rassert(base->began_waiting_for_flush_);
        rassert(!base->spawned_flush_);
        ASSERT_FINITE_CORO_WAITING;

        std::vector<page_txn_t *> flush_set = page_cache_t::maximal_flushable_txn_set(base);
        if (!flush_set.empty())
        {
            for (auto it = flush_set.begin(); it != flush_set.end(); ++it)
            {
                rassert(!(*it)->spawned_flush_);
                (*it)->spawned_flush_ = true;
            }

            std::map<block_id_t, block_change_t> changes = page_cache_t::compute_changes(flush_set);

            if (!changes.empty())
            {
                coro_t::spawn_now_dangerously(std::bind(&page_cache_t::do_flush_txn_set,
                                                        this,
                                                        &changes,
                                                        flush_set));
            }
            else
            {
                // Flush complete.  do_flush_txn_set does this in the write case.
                page_cache_t::remove_txn_set_from_graph(this, flush_set);
            }
        }
    }

} // namespace alt
