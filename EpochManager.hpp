#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <new>
#include <vector>
#include <algorithm>

struct alignas(64) ThreadLocalState
{
    std::atomic<uint64_t> epoch{0};
    std::atomic<bool> active{false};

    struct RetiredItem
    {
        void *ptr;
        uint64_t retire_epoch;
        std::function<void(void *)> reclaimer;
    };
    std::vector<RetiredItem> local_retired;

    ThreadLocalState() { local_retired.reserve(256); }
};

class EpochManager
{
public:
    using ReclaimFunc = std::function<void(void *)>;

    static EpochManager &instance() noexcept
    {
        static EpochManager inst;
        return inst;
    }

    // ✅ 修复：将递归改为 while 循环，彻底杜绝高并发下的栈溢出
    uint64_t pin() noexcept
    {
        auto &local = get_local_state();
        while (true) {
            uint64_t e = global_epoch_.load(std::memory_order_relaxed);
            local.epoch.store(e, std::memory_order_relaxed);
            local.active.store(true, std::memory_order_release);
            
            // 全内存屏障，确保 active=true 在读取 global_epoch 之前被其他线程看到
            std::atomic_thread_fence(std::memory_order_seq_cst); 
            
            if (e == global_epoch_.load(std::memory_order_acquire)) {
                return e; // 成功 pin
            }
            // epoch 发生变化，取消 active 并重试
            local.active.store(false, std::memory_order_relaxed); 
        }
    }

    void unpin() noexcept
    {
        auto &local = get_local_state();
        local.active.store(false, std::memory_order_release);
    }

    void advance_epoch() noexcept
    {
        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }

    void retire(void *ptr, ReclaimFunc reclaimer)
    {
        if (!ptr) return;
        auto &local = get_local_state();
        uint64_t e = global_epoch_.load(std::memory_order_relaxed);
        local.local_retired.push_back({ptr, e, std::move(reclaimer)});

        if (local.local_retired.size() >= kRetireThreshold)
        {
            advance_epoch();
            try_reclaim_local(local);
        }
    }

    void try_reclaim()
    {
        auto &local = get_local_state();
        try_reclaim_local(local);
    }

    void force_reclaim_all()
    {
        std::lock_guard<std::mutex> lk(states_mtx_);
        for (auto *state : all_states_)
        {
            for (auto &item : state->local_retired)
            {
                if (item.ptr) { item.reclaimer(item.ptr); item.ptr = nullptr; }
            }
            state->local_retired.clear();
        }
    }

    void force_reclaim_local() noexcept
    {
        auto &local = get_local_state();
        for (auto &item : local.local_retired)
        {
            if (item.ptr) { item.reclaimer(item.ptr); item.ptr = nullptr; }
        }
        local.local_retired.clear();
    }

private:
    EpochManager() = default;
    ~EpochManager()
    {
        force_reclaim_all();
        std::lock_guard<std::mutex> lk(states_mtx_);
        for (auto *state : all_states_)
        {
            state->~ThreadLocalState();
            ::operator delete(static_cast<void *>(state),
                              std::align_val_t(alignof(ThreadLocalState)));
        }
        all_states_.clear();
    }

    EpochManager(const EpochManager &) = delete;
    EpochManager &operator=(const EpochManager &) = delete;

    static constexpr size_t kRetireThreshold = 128;

    ThreadLocalState &get_local_state()
    {
        thread_local ThreadLocalState *tls_ptr = nullptr;
        if (!tls_ptr)
        {
            void *raw = ::operator new(sizeof(ThreadLocalState),
                                       std::align_val_t(alignof(ThreadLocalState)));
            tls_ptr = new (raw) ThreadLocalState();
            std::lock_guard<std::mutex> lk(states_mtx_);
            all_states_.push_back(tls_ptr);
        }
        return *tls_ptr;
    }

    void try_reclaim_local(ThreadLocalState &local)
    {
        if (local.local_retired.empty()) return;

        uint64_t min_active = global_epoch_.load(std::memory_order_acquire);

        std::vector<ThreadLocalState *> snapshot;
        {
            std::lock_guard<std::mutex> lk(states_mtx_);
            snapshot = all_states_;
        }

        for (auto *state : snapshot)
        {
            if (state->active.load(std::memory_order_acquire))
            {
                uint64_t e = state->epoch.load(std::memory_order_acquire);
                if (e < min_active) min_active = e;
            }
        }

        std::vector<ThreadLocalState::RetiredItem> to_reclaim;
        std::vector<ThreadLocalState::RetiredItem> to_keep;
        to_keep.reserve(local.local_retired.size());

        for (auto &item : local.local_retired)
        {
            if (item.retire_epoch < min_active)
                to_reclaim.push_back(std::move(item));
            else
                to_keep.push_back(std::move(item));
        }

        local.local_retired = std::move(to_keep);

        for (auto &item : to_reclaim)
        {
            if (item.ptr)
                item.reclaimer(item.ptr);
        }
    }

    std::atomic<uint64_t> global_epoch_{1};
    std::mutex states_mtx_;
    std::vector<ThreadLocalState *> all_states_;
};

class EpochGuard
{
public:
    explicit EpochGuard(EpochManager &mgr = EpochManager::instance()) noexcept
        : mgr_(mgr) { mgr_.pin(); }
    ~EpochGuard() { mgr_.unpin(); }
    EpochGuard(const EpochGuard &) = delete;
    EpochGuard &operator=(const EpochGuard &) = delete;
    EpochGuard(EpochGuard &&) = delete;
    EpochGuard &operator=(EpochGuard &&) = delete;
private:
    EpochManager &mgr_;
};