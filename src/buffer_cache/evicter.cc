#include "buffer_cache/evicter.hpp"

#include "arch/runtime/coroutines.hpp"
#include "buffer_cache/alt.hpp"
#include "buffer_cache/page.hpp"
#include "buffer_cache/page_cache.hpp"
#include "buffer_cache/cache_balancer.hpp"

namespace alt
{

    evicter_t::evicter_t()
        : initialized_(false),
          page_cache_(nullptr),
          balancer_(nullptr),
          balancer_notify_activity_boolean_(nullptr),
          throttler_(nullptr),
          bytes_loaded_counter_(0),
          access_count_counter_(0),
          access_time_counter_(INITIAL_ACCESS_TIME),
          evict_if_necessary_active_(false) {}

    evicter_t::~evicter_t()
    {
        assert_thread();
        drainer_.drain();
        if (initialized_)
        {
            balancer_->remove_evicter(this);
        }
        guarantee(!evict_if_necessary_active_);
    }

    void evicter_t::initialize(page_cache_t *page_cache,
                               cache_balancer_t *balancer,
                               alt_txn_throttler_t *throttler)
    {
        assert_thread();
        guarantee(balancer != nullptr);
        initialized_ = true; // Can you really say this class is 'initialized_'?
        page_cache_ = page_cache;
        memory_limit_ = balancer->base_mem_per_store();
        page_cache_ = page_cache;
        throttler_ = throttler;
        balancer_ = balancer;
        balancer_notify_activity_boolean_ = balancer_->notify_activity_boolean(get_thread_id());
        balancer_->add_evicter(this);
        throttler_->inform_memory_limit_change(memory_limit_,
                                               page_cache_->max_block_size());
    }

    void evicter_t::update_memory_limit(uint64_t new_memory_limit,
                                        int64_t bytes_loaded_accounted_for,
                                        uint64_t access_count_accounted_for,
                                        bool read_ahead_ok)
    {
        assert_thread();
        guarantee(initialized_);

        if (!read_ahead_ok)
        {
            page_cache_->have_read_ahead_cb_destroyed();
        }

        bytes_loaded_counter_ -= bytes_loaded_accounted_for;
        access_count_counter_ -= access_count_accounted_for;
        memory_limit_ = new_memory_limit;
        evict_if_necessary();

        throttler_->inform_memory_limit_change(memory_limit_,
                                               page_cache_->max_block_size());
    }

    int64_t evicter_t::get_bytes_loaded() const
    {
        assert_thread();
        guarantee(initialized_);
        return bytes_loaded_counter_;
    }

    uint64_t evicter_t::memory_limit() const
    {
        assert_thread();
        guarantee(initialized_);
        return memory_limit_;
    }

    uint64_t evicter_t::access_count() const
    {
        assert_thread();
        guarantee(initialized_);
        return access_count_counter_;
    }

    void wake_up_balancer(cache_balancer_t *balancer,
                          UNUSED auto_drainer_t::lock_t drainer_lock)
    {
        on_thread_t th(balancer->home_thread());
        balancer->wake_up_activity_happened();
    }

    void evicter_t::notify_bytes_loading(int64_t in_memory_buf_change)
    {
        assert_thread();
        guarantee(initialized_);
        bytes_loaded_counter_ += in_memory_buf_change;
        access_count_counter_ += 1;
        if (*balancer_notify_activity_boolean_)
        {
            *balancer_notify_activity_boolean_ = false;

            coro_t::spawn_sometime(std::bind(&wake_up_balancer,
                                             balancer_,
                                             drainer_.lock()));
        }
    }

    void evicter_t::add_deferred_loaded(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        evicted_.add(page, page->hypothetical_memory_usage(page_cache_));
    }

    void evicter_t::catch_up_deferred_load(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        rassert(unevictable_.has_page(page));
        notify_bytes_loading(page->hypothetical_memory_usage(page_cache_));
    }

    void evicter_t::add_not_yet_loaded(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        unevictable_.add(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
        notify_bytes_loading(page->hypothetical_memory_usage(page_cache_));
    }

    void evicter_t::reloading_page(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        notify_bytes_loading(page->hypothetical_memory_usage(page_cache_));
    }

    bool evicter_t::page_is_in_unevictable_bag(page_t *page) const
    {
        assert_thread();
        guarantee(initialized_);
        return unevictable_.has_page(page);
    }

    bool evicter_t::page_is_in_evicted_bag(page_t *page) const
    {
        assert_thread();
        guarantee(initialized_);
        return evicted_.has_page(page);
    }

    void evicter_t::add_to_evictable_unbacked(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        evictable_unbacked_.add(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
        notify_bytes_loading(page->hypothetical_memory_usage(page_cache_));
    }

    void evicter_t::add_to_evictable_rdma(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        rdma_bag_.add(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
        // notify_bytes_loading(page->hypothetical_memory_usage(page_cache_));
    }

    void evicter_t::add_to_evictable_disk_backed(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        evictable_disk_backed_.add(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
        notify_bytes_loading(page->hypothetical_memory_usage(page_cache_));
    }

    void evicter_t::move_unevictable_to_evictable(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        rassert(unevictable_.has_page(page));
        unevictable_.remove(page, page->hypothetical_memory_usage(page_cache_));
        eviction_bag_t *new_bag = correct_eviction_category(page);
        rassert(new_bag == &evictable_disk_backed_ || new_bag == &evictable_unbacked_);
        new_bag->add(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
    }

    void evicter_t::change_to_correct_eviction_bag(eviction_bag_t *current_bag,
                                                   page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        if (page->is_rdma_page())
        {
            // Place RDMA pages in the RDMA bag
            return;
        }
        rassert(current_bag->has_page(page));
        current_bag->remove(page, page->hypothetical_memory_usage(page_cache_));
        eviction_bag_t *new_bag = correct_eviction_category(page);
        new_bag->add(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
    }

    eviction_bag_t *evicter_t::correct_eviction_category(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        if (page->is_rdma_page())
        {
            // Place RDMA pages in the RDMA bag
            return &rdma_bag_;
        }
        else if (page->is_loading() || page->has_waiters())
        {
            return &unevictable_;
        }
        else if (!page->is_loaded())
        {
            return &evicted_;
        }
        else if (page->is_disk_backed())
        {
            if (page_cache_->check_if_in_current_pages(page->block_id()))
            {
                return &evictable_disk_backed_;
            }
            return &rdma_bag_;
        }
        else
        {
            return &evictable_unbacked_;
        }
    }

    void evicter_t::remove_page(page_t *page)
    {
        assert_thread();
        guarantee(initialized_);
        eviction_bag_t *bag = correct_eviction_category(page);
        bag->remove(page, page->hypothetical_memory_usage(page_cache_));
        evict_if_necessary();
    }

    uint64_t evicter_t::in_memory_size() const
    {
        assert_thread();
        guarantee(initialized_);
        return unevictable_.size() + evictable_disk_backed_.size() + evictable_unbacked_.size();
    }

    void evicter_t::evict_if_necessary() THROWS_NOTHING
    {
        assert_thread();
        guarantee(initialized_);
        if (evict_if_necessary_active_)
        {
            // Reentrant call to evict_if_necessary().
            // There is no need to start another eviction loop, plus we want to avoid
            // recursive reentrant calls for reasons of correctness and to avoid stack
            // overflows.
            return;
        }
        // KSI: Implement eviction of unbacked evictables too.  When flushing, you
        // could use the page_t::eviction_index_ field to identify pages that are
        // currently in the process of being evicted, to avoid reflushing a page
        // currently being written for the purpose of eviction.

        evict_if_necessary_active_ = true;
        page_t *page;
        while (in_memory_size() > memory_limit_ && evictable_disk_backed_.remove_oldish(&page, access_time_counter_,
                                                                                        page_cache_))
        {
            if (page->is_rdma_page())
            {
                continue;
            }
            evicted_.add(page, page->hypothetical_memory_usage(page_cache_));
            page->evict_self(page_cache_);
            page_cache_->consider_evicting_current_page(page->block_id());
        }
        if (WRITES_ENABLED)
        {
            evict_writes();
        }

        evict_if_necessary_active_ = false;
    }

    void evicter_t::evict_writes() THROWS_NOTHING
    {
        guarantee(initialized_);
        page_t *page;
        auto &ebag = evictable_disk_backed_.bag_;
        if (ebag.size() > 25)
        {
            std::vector<page_t *> iter_pages;
            for (size_t i = 0; i < ebag.size(); i++)
            {
                page_t *iter_page = ebag.access_random(i);
                iter_pages.emplace_back(iter_page);
            }
            for (size_t i = 0; i < ebag.size(); i++)
            {
                page_t *iter_page = iter_pages[i];
                page = iter_page;
                if (iter_page->is_write)
                {
                    evictable_disk_backed_.remove(iter_page, iter_page->hypothetical_memory_usage(page_cache_));
                    evicted_.add(page, page->hypothetical_memory_usage(page_cache_));
                    page->evict_self(page_cache_);
                    page_cache_->erase_write_page_for_block_id(page->block_id());
                }
            }
        }
    }

    void evicter_t::remove_out_of_range_pages_periodically()
    {
        assert_thread();
        guarantee(initialized_);
        std::cout << "Starting periodic eviction" << std::endl;
        for (auto &&page_ : page_cache_->getCurrentPages())
        {
            uint64_t i = page_.first;
            // std::cout << "Checking page with block_id: " << i << std::endl;
            if (page_cache_->check_if_block_duplicate(i))
            {
                // auto it = page_cache_->getCurrentPages().find(i);
                // if (it == page_cache_->getCurrentPages().end())
                // {
                //     continue;
                // }
                // page_t *page = it->second->the_page_for_read_for_RDMA();
                page_t *page = page_.second->the_page_for_read_for_RDMA();
                if (page->is_loaded())
                {
                    if (page->is_rdma_page())
                    {
                        continue;
                    }
                    if (page_cache_->check_if_internal_page(page))
                    {
                        continue;
                    }
                    if (page->block_id() >= 0 && page->block_id() <= 2)
                    {
                        continue;
                    }
                    if (evictable_disk_backed_.has_page(page))
                    {
                        evictable_disk_backed_.remove(page, page->hypothetical_memory_usage(page_cache_));
                        if (page->block_token().has())
                        {
                            evicted_.add(page, page->hypothetical_memory_usage(page_cache_));
                        }
                        page->evict_self(page_cache_);
                        page_cache_->consider_evicting_current_page(page->block_id());
                    }
                }
            }
        }
        std::cout << "Periodic Eviction done" << std::endl;
        page_cache_->print_current_pages_to_file(page_cache_->getPageMap()->file_number);
        page_cache_->getPageMap()->file_number++;
    }

    void evicter_t::remove_non_leaf_before_read()
    {
        return;
        assert_thread();
        guarantee(initialized_);
        std::cout << "Starting eviction" << std::endl;
        for (auto &&page_ : page_cache_->getCurrentPages())
        {
            uint64_t i = page_.first;
            // std::cout << "Checking page with block_id: " << i << std::endl;
            page_t *page = page_.second->the_page_for_read_for_RDMA();
            // if (page->is_loaded() && !page_cache_->check_if_node_in_range(page->block_id()))
            if (page->is_loaded())
            {
                if (page->is_rdma_page())
                {
                    continue;
                }
                if (page->is_loaded())
                {
                    if (page_cache_->check_if_internal_page(page))
                    {
                        continue;
                    }
                }
                if (evictable_disk_backed_.has_page(page))
                {
                    evictable_disk_backed_.remove(page, page->hypothetical_memory_usage(page_cache_));
                    if (page->block_token().has())
                    {
                        evicted_.add(page, page->hypothetical_memory_usage(page_cache_));
                    }
                    page->evict_self(page_cache_);
                    page_cache_->consider_evicting_current_page(page->block_id());
                }
            }
        }
        std::cout << "Eviction done" << std::endl;
        page_cache_->print_current_pages_to_file(page_cache_->getPageMap()->file_number);
        page_cache_->getPageMap()->file_number++;
    }

    void evicter_t::print_all_bag_sizes()
    {
        std::cout << "RDMA bags: " << rdma_bag_.size() / 4096 << " Unevictable bags: "
                  << unevictable_.size() / 4096 << " Evicted bags: " << evicted_.size() / 4096
                  << " Evictable disk backed bags: " << evictable_disk_backed_.size() / 4096
                  << " Evictable unbacked bags: " << evictable_unbacked_.size() / 4096 << std::endl;
    }

    usage_adjuster_t::usage_adjuster_t(page_cache_t *page_cache, page_t *page)
        : page_cache_(page_cache),
          page_(page),
          eviction_bag_(page_cache->evicter().correct_eviction_category(page)),
          original_usage_(page->hypothetical_memory_usage(page_cache)) {}

    usage_adjuster_t::~usage_adjuster_t()
    {
        int64_t old64 = original_usage_;
        int64_t new64 = page_->hypothetical_memory_usage(page_cache_);
        int64_t adjustment = new64 - old64;
        eviction_bag_->change_size(adjustment);
        page_cache_->evicter().evict_if_necessary();
        page_cache_->evicter().notify_bytes_loading(adjustment);
    }

} // namespace alt
