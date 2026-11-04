#pragma once
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <utility>
#include <concepts>
#include <type_traits>
#include <random>

namespace mem_pool
{
    template <typename T>
    struct alignas(T) MemNode
    {
        union
        {
            T unit;
            MemNode *next;
        };
        
#ifdef MEM_POOL_DEBUG
        static constexpr uint64_t FREE_MAGIC = 0xDEAD'BEEF'CAFE'BABEULL;
        uint64_t debug_magic;
        void MarkFree() noexcept { debug_magic = FREE_MAGIC; }
        [[nodiscard]] bool IsFree() const noexcept { return debug_magic == FREE_MAGIC; }
        void MarkUsed() noexcept { debug_magic = 0; }
#else
        void MarkFree() noexcept {}
        [[nodiscard]] bool IsFree() const noexcept { return true; }
        void MarkUsed() noexcept {}
#endif
    };

    template <typename T>
    class LockFreeMemPool
    {
    public:
        using NodeType = MemNode<T>;
        LockFreeMemPool() : free_front(0) {
            static_assert(std::atomic<uint64_t>::is_always_lock_free, "uint64_t atomic must be lock-free");
        }

        NodeType *AllocAll() noexcept {
            uint64_t old_val = free_front.load(std::memory_order_relaxed);
            while ((old_val & kAddressMask) != 0) {
                if (free_front.compare_exchange_weak(old_val, 0, std::memory_order_acquire, std::memory_order_relaxed))
                    return reinterpret_cast<NodeType *>(old_val & kAddressMask);
            }
            return nullptr;
        }

        void Free(NodeType *node) {
            if (node == nullptr) return;
            uint64_t old_val, new_val;
            do {
                old_val = free_front.load(std::memory_order_relaxed);
                node->next = reinterpret_cast<NodeType *>(old_val & kAddressMask);
                uint16_t new_tag = static_cast<uint16_t>(old_val >> 48) + 1;
                new_val = reinterpret_cast<uint64_t>(node) | (static_cast<uint64_t>(new_tag) << 48);
            } while (!free_front.compare_exchange_weak(old_val, new_val, std::memory_order_release, std::memory_order_relaxed));
        }

        void FreeChain(NodeType *head, NodeType *tail) {
            if (head == nullptr) return;
            uint64_t old_val, new_val;
            do {
                old_val = free_front.load(std::memory_order_relaxed);
                tail->next = reinterpret_cast<NodeType *>(old_val & kAddressMask);
                uint16_t new_tag = static_cast<uint16_t>(old_val >> 48) + 1;
                new_val = reinterpret_cast<uint64_t>(head) | (static_cast<uint64_t>(new_tag) << 48);
            } while (!free_front.compare_exchange_weak(old_val, new_val, std::memory_order_release, std::memory_order_relaxed));
        }

    private:
        alignas(64) std::atomic<uint64_t> free_front; 
        static constexpr uint64_t kAddressMask = 0x0000'FFFF'FFFF'FFFF;
    };
} 

template <typename T>
concept Poolable = std::is_nothrow_destructible_v<T> && !std::is_array_v<T> && sizeof(T) >= sizeof(void *);

template <Poolable T> class TlsMemPoolManager;

template <typename T>
class TlsMemPool
{
    friend class TlsMemPoolManager<T>;
public:
    using NodeType = mem_pool::MemNode<T>;
    TlsMemPool(const TlsMemPool &) = delete;
    TlsMemPool &operator=(const TlsMemPool &) = delete;

    template <typename... Args> T *Alloc(Args &&...args);
    void Free(T *address); // Deprecated, use Manager

private:
    TlsMemPool(size_t unit_cnt, size_t capacity, TlsMemPoolManager<T> *mgr)
        : mem_pool_mgr(mgr), free_front(nullptr), unit_count(0), unit_capacity(capacity), lf_pool(nullptr) {
        if (unit_capacity == 0) unit_capacity = 64;
    }
    ~TlsMemPool() { free_front = nullptr; unit_count = 0; lf_pool = nullptr; }

    void InjectChain(NodeType *head) {
        NodeType *tail = head; size_t n = 1;
        while (tail->next) { tail = tail->next; ++n; }
        tail->next = free_front; free_front = head; unit_count += n;
    }

    void OverflowToLfPool() {
        const size_t keep = unit_capacity / 2;
        NodeType *cur = free_front;
        for (size_t i = 1; i < keep && cur != nullptr; ++i) cur = cur->next;
        if (cur == nullptr || cur->next == nullptr) return;
        NodeType *moved_head = cur->next; cur->next = nullptr;
        NodeType *tail = moved_head;
        while (tail->next) tail = tail->next;
        lf_pool->FreeChain(moved_head, tail); 
        unit_count = keep;
    }

    TlsMemPoolManager<T> *mem_pool_mgr;
    NodeType *free_front;
    size_t unit_count;
    size_t unit_capacity;
    mem_pool::LockFreeMemPool<T> *lf_pool; 
};

template <Poolable T>
class TlsMemPoolManager
{
    friend class TlsMemPool<T>;
    using PoolType = TlsMemPool<T>;
    using NodeType = mem_pool::MemNode<T>;
    using LockFreePoolType = mem_pool::LockFreeMemPool<T>;

public:
    static constexpr size_t MEM_POOL_BATCH_ALLOC_SIZE = 64;

    TlsMemPoolManager() : TlsMemPoolManager(0, 0) {}
    explicit TlsMemPoolManager(size_t default_unit_cnt, size_t default_unit_capacity)
        : default_unit_cnt(default_unit_cnt), default_unit_capacity(default_unit_capacity), batch_alloc_size(MEM_POOL_BATCH_ALLOC_SIZE) {}

    ~TlsMemPoolManager() {
        {
            std::lock_guard<std::mutex> lk(memory_blocks_mutex);
            for (void *p : all_memory_blocks) ::operator delete(p, std::align_val_t(alignof(NodeType)));
            all_memory_blocks.clear();
        }
        {
            std::unique_lock<std::shared_mutex> lk(thread_pools_mutex);
            for (auto &kv : thread_pools) { delete kv.second.tls_pool; delete kv.second.lf_pool; }
            thread_pools.clear();
        }
    }

    static TlsMemPoolManager<T> &instance() {
        static TlsMemPoolManager<T> inst;
        return inst;
    }

    TlsMemPoolManager(const TlsMemPoolManager &) = delete;
    TlsMemPoolManager &operator=(const TlsMemPoolManager &) = delete;

    void InitThreadLocalPool(size_t unit_cnt = 0, size_t unit_capacity = 0);
    void ReleaseThreadLocalPool();

    template <typename... Args>
    T *Alloc(Args &&...args) {
        if (tls_mem_pool == nullptr) InitThreadLocalPool();
        return tls_mem_pool->Alloc(std::forward<Args>(args)...);
    }
    
    void Free(T *p) {
        if (p == nullptr) return;
        p->~T(); 
        NodeType *node = reinterpret_cast<NodeType *>(p);
        node->MarkFree();

        if (tls_mem_pool != nullptr && tls_mem_pool->mem_pool_mgr == this) {
            node->next = tls_mem_pool->free_front;
            tls_mem_pool->free_front = node;
            tls_mem_pool->unit_count++;
            if (tls_mem_pool->unit_count > tls_mem_pool->unit_capacity) tls_mem_pool->OverflowToLfPool();
        } else {
            global_free_pool.Free(node);
        }
    }

private:
    void RequestAlloc(PoolType *pool);
    NodeType *StealFromOtherThreads(PoolType *pool);
    void ExpandWithNewBlock(PoolType *pool);
    
    void RegisterMemoryBlock(void *block) {
        std::lock_guard<std::mutex> lk(memory_blocks_mutex);
        all_memory_blocks.push_back(block);
    }

    struct ThreadPoolInfo { LockFreePoolType *lf_pool; PoolType *tls_pool; };

    static thread_local PoolType *tls_mem_pool;
    static thread_local LockFreePoolType *tls_lf_pool;

    LockFreePoolType global_free_pool;     
    std::vector<void *> all_memory_blocks; 
    std::mutex memory_blocks_mutex;
    std::shared_mutex thread_pools_mutex;
    std::unordered_map<std::thread::id, ThreadPoolInfo> thread_pools; 

    size_t default_unit_cnt;
    size_t default_unit_capacity;
    size_t batch_alloc_size;
};

template <Poolable T> thread_local TlsMemPool<T> *TlsMemPoolManager<T>::tls_mem_pool = nullptr;
template <Poolable T> thread_local mem_pool::LockFreeMemPool<T> *TlsMemPoolManager<T>::tls_lf_pool = nullptr;

template <typename T>
template <typename... Args>
T *TlsMemPool<T>::Alloc(Args &&...args) {
    if (free_front == nullptr) {
        NodeType *head = lf_pool->AllocAll();
        if (head != nullptr) InjectChain(head);
        if (free_front == nullptr) mem_pool_mgr->RequestAlloc(this);
        if (free_front == nullptr) return nullptr; 
    }
    NodeType *node = free_front;
    free_front = node->next;
    --unit_count;
    T *ptr = reinterpret_cast<T *>(node); 
    try { ::new (static_cast<void *>(ptr)) T(std::forward<Args>(args)...); }
    catch (...) { node->next = free_front; free_front = node; ++unit_count; throw; }
    node->MarkUsed(); 
    return ptr;
}

template <typename T> void TlsMemPool<T>::Free(T *address) { (void)address; }

template <Poolable T>
void TlsMemPoolManager<T>::InitThreadLocalPool(size_t unit_cnt, size_t unit_capacity) {
    if (tls_mem_pool != nullptr) return; 
    if (unit_cnt == 0) unit_cnt = default_unit_cnt;
    if (unit_capacity == 0) unit_capacity = default_unit_capacity;

    auto *new_pool = new PoolType(unit_cnt, unit_capacity, this);
    auto *new_lf = new LockFreePoolType();
    new_pool->lf_pool = new_lf; 
    tls_mem_pool = new_pool;
    tls_lf_pool = new_lf;

    std::unique_lock<std::shared_mutex> lk(thread_pools_mutex);
    thread_pools[std::this_thread::get_id()] = ThreadPoolInfo{new_lf, new_pool};
}

template <Poolable T>
void TlsMemPoolManager<T>::ReleaseThreadLocalPool() {
    if (tls_mem_pool == nullptr || tls_mem_pool->mem_pool_mgr != this) return; 

    NodeType *local_head = tls_mem_pool->free_front;
    if (local_head != nullptr) {
        NodeType *tail = local_head;
        while (tail->next) tail = tail->next;
        global_free_pool.FreeChain(local_head, tail);
    }
    tls_mem_pool->free_front = nullptr;
    tls_mem_pool->unit_count = 0;

    NodeType *lf_head = tls_lf_pool->AllocAll();
    if (lf_head != nullptr) {
        NodeType *tail = lf_head;
        while (tail->next) tail = tail->next;
        global_free_pool.FreeChain(lf_head, tail);
    }

    {
        std::unique_lock<std::shared_mutex> lk(thread_pools_mutex);
        thread_pools.erase(std::this_thread::get_id());
    }

    delete tls_mem_pool; tls_mem_pool = nullptr;
    delete tls_lf_pool; tls_lf_pool = nullptr;
}

template <Poolable T>
void TlsMemPoolManager<T>::RequestAlloc(PoolType *pool) {
    NodeType *head = global_free_pool.AllocAll();
    if (head != nullptr) { pool->InjectChain(head); return; }
    head = StealFromOtherThreads(pool);
    if (head != nullptr) { pool->InjectChain(head); return; }
    ExpandWithNewBlock(pool);
}

template <Poolable T>
typename TlsMemPoolManager<T>::NodeType * TlsMemPoolManager<T>::StealFromOtherThreads(PoolType *pool) {
    std::shared_lock<std::shared_mutex> lk(thread_pools_mutex);
    if (thread_pools.size() <= 1) return nullptr;
    thread_local std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    size_t start = rng() % thread_pools.size();
    size_t idx = 0;
    for (auto it = thread_pools.begin(); it != thread_pools.end(); ++it, ++idx) {
        auto actual_it = std::next(thread_pools.begin(), (start + idx) % thread_pools.size());
        if (actual_it->second.lf_pool == pool->lf_pool) continue;
        NodeType *head = actual_it->second.lf_pool->AllocAll();
        if (head != nullptr) return head;
    }
    return nullptr;
}

template <Poolable T>
void TlsMemPoolManager<T>::ExpandWithNewBlock(PoolType *pool) {
    size_t n = pool->unit_capacity > batch_alloc_size ? pool->unit_capacity : batch_alloc_size;
    if (n == 0) n = MEM_POOL_BATCH_ALLOC_SIZE;
    void *raw = ::operator new(n * sizeof(NodeType), std::align_val_t(alignof(NodeType)));
    RegisterMemoryBlock(raw); 
    NodeType *nodes = static_cast<NodeType *>(raw);
    for (size_t i = 0; i + 1 < n; ++i) nodes[i].next = &nodes[i + 1];
    nodes[n - 1].next = nullptr;
    pool->InjectChain(nodes);
}