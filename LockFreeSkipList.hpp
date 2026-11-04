#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>
#include <random>
#include <thread>
#include "DataHandle.h"
#include "EpochManager.hpp"
#include "MemPool.hpp"

// =====================================================================
// 无锁跳表（MemTable）—— 追加式版本链（Append-Only）设计
//
// 设计变更（修复 16 字节原子依赖）：
// 前版把 (value, seq) 打包成 16 字节用单个 128 位原子 CAS 覆盖更新，
// 但这依赖 cmpxchg16b，在部分 CPU / 编译环境（libstdc++ 判定非 lock-free）
// 无法通过 static_assert。
//
// 本版改为 LevelDB 同款"追加式版本链"：
//   - 同一 key 的多个版本不再原地覆盖，而是按插入顺序连成一个版本运行（run），
//     每个节点在链接后 (key, value, seq) 全部不可变 → 从构造上消灭撕裂窗口，
//     不需要任何 16 字节原子；
//   - 读路径沿 run 遍历取 seq 最大者；
//   - 写路径在插入时即可确定"哪个版本成为垃圾"（见 insert 返回值），
//     供 DB::Put/Delete 就地记 discardable —— 这就是版本链记账。
//
// 并发正确性：节点在 level-0 CAS 链接前完整初始化；读者通过 acquire 读
// forward 指针后才能看见节点，因此看到的永远是完整的不可变三元组。
// =====================================================================
template <typename KeyType, typename Compare = std::less<KeyType>>
class LockFreeSkipList
{
private:
    static constexpr int MAX_LEVEL = 12;
    static constexpr double P = 0.25;
    static constexpr int MAX_CAS_RETRIES = 32;

    struct Node
    {
        KeyType key;
        uint64_t value; // 不可变（链接后不再修改）
        uint64_t seq;   // 不可变
        int top_level;
        std::atomic<Node *> forward[MAX_LEVEL];

        Node() noexcept : key(KeyType{}), value(0), seq(0), top_level(-1) {}
        Node(const KeyType &k, uint64_t v, uint64_t s, int level)
            : key(k), value(v), seq(s), top_level(level)
        {
            for (int i = 0; i <= top_level; ++i)
                forward[i].store(nullptr, std::memory_order_relaxed);
        }
    };

    using NodePool = TlsMemPoolManager<Node>;
    Node *head_;
    Compare comp_;

    bool key_equal(const KeyType &a, const KeyType &b) const
    {
        return !comp_(a, b) && !comp_(b, a);
    }

    int random_level() noexcept
    {
        thread_local std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
        int lvl = 0;
        while (dist(rng) < P && lvl < MAX_LEVEL - 1)
            lvl++;
        return lvl;
    }

public:
    using NodeType = Node;
    Node *GetHead() const noexcept { return head_; }

    // 定位到 >= key 的第一个节点（同 key 版本运行的头）
    Node* Seek(const KeyType& key) const {
        EpochGuard guard;
        Node* current = head_;
        for (int i = head_->top_level; i >= 0; i--) {
            Node* next = current->forward[i].load(std::memory_order_acquire);
            while (next != nullptr && comp_(next->key, key)) {
                current = next;
                next = current->forward[i].load(std::memory_order_acquire);
            }
        }
        return current->forward[0].load(std::memory_order_acquire);
    }

    explicit LockFreeSkipList(Compare comp = Compare()) : comp_(comp)
    {
        NodePool::instance().InitThreadLocalPool();
        head_ = NodePool::instance().Alloc();
        head_->top_level = MAX_LEVEL - 1;
        head_->key = KeyType{};
        head_->value = 0;
        head_->seq = 0;
        for (int i = 0; i < MAX_LEVEL; i++)
            head_->forward[i].store(nullptr, std::memory_order_relaxed);
    }

    ~LockFreeSkipList()
    {
        Node *curr = head_->forward[0].load(std::memory_order_relaxed);
        while (curr)
        {
            Node *next = curr->forward[0].load(std::memory_order_relaxed);
            NodePool::instance().Free(curr);
            curr = next;
        }
        NodePool::instance().Free(head_);
    }

    // ✅ 版本链记账接口：
    //   - 新 key：返回 std::nullopt（没有版本成为垃圾）
    //   - 同 key 且本次 seq 更大：返回被取代的旧"最新版本"raw
    //   - 同 key 但本次 seq 更小（并发乱序插入）：本次版本立即作废，返回自身 raw
    std::optional<uint64_t> Put(const KeyType &key, const DataHandle &dh, uint64_t seq = 0)
    {
        return insert(key, dh.raw, seq);
    }

    // 读：沿版本链取 seq 最大的版本
    bool GetHandle(const KeyType &key, uint64_t &out_raw, uint64_t &out_seq) const
    {
        EpochGuard guard;
        Node *current = head_;
        for (int i = head_->top_level; i >= 0; i--)
        {
            Node *next = current->forward[i].load(std::memory_order_acquire);
            while (next != nullptr && comp_(next->key, key))
            {
                current = next;
                next = current->forward[i].load(std::memory_order_acquire);
            }
        }
        current = current->forward[0].load(std::memory_order_acquire);

        bool found = false;
        uint64_t best_raw = 0, best_seq = 0;
        while (current != nullptr && key_equal(current->key, key))
        {
            if (!found || current->seq > best_seq)
            {
                best_seq = current->seq;
                best_raw = current->value;
                found = true;
            }
            current = current->forward[0].load(std::memory_order_acquire);
        }
        if (found)
        {
            out_raw = best_raw;
            out_seq = best_seq;
            return true;
        }
        return false;
    }

private:
    std::optional<uint64_t> insert(const KeyType &key, uint64_t val_raw, uint64_t seq)
    {
        Node *preds[MAX_LEVEL];
        Node *succs[MAX_LEVEL];
        while (true)
        {
            // 搜索：越过所有 key <= target 的节点，停在"版本运行"的尾部
            //（新版本追加在 run 末尾，LevelDB 式 append-only）
            Node *current = head_;
            Node *newest_same_key = nullptr;
            uint64_t newest_seq = 0;
            for (int i = head_->top_level; i >= 0; i--)
            {
                Node *next = current->forward[i].load(std::memory_order_acquire);
                while (next != nullptr && !comp_(key, next->key))   // next->key <= key
                {
                    // level 0 会遍历完整 run，顺手记录 seq 最大的同 key 节点
                    if (i == 0 && key_equal(next->key, key) && next->seq > newest_seq)
                    {
                        newest_same_key = next;
                        newest_seq = next->seq;
                    }
                    current = next;
                    next = current->forward[i].load(std::memory_order_acquire);
                }
                preds[i] = current;
                succs[i] = next;
            }

            int new_level = random_level();
            Node *new_node = NodePool::instance().Alloc(key, val_raw, seq, new_level);
            for (int i = 0; i <= new_level; i++)
                new_node->forward[i].store(succs[i], std::memory_order_relaxed);

            Node *expected = succs[0];
            if (!preds[0]->forward[0].compare_exchange_strong(expected, new_node,
                                                              std::memory_order_release,
                                                              std::memory_order_relaxed))
            {
                NodePool::instance().Free(new_node);
                std::this_thread::yield();
                continue; // 重试：重新走搜索，重新确定"最新版本"
            }
            for (int i = 1; i <= new_level; i++)
            {
                int retries = 0;
                while (true)
                {
                    Node *exp = succs[i];
                    if (preds[i]->forward[i].compare_exchange_weak(exp, new_node,
                                                                   std::memory_order_release,
                                                                   std::memory_order_relaxed))
                        break;
                    if (++retries > MAX_CAS_RETRIES)
                    {
                        std::this_thread::yield();
                        retries = 0;
                    }
                    Node *cur = preds[i];
                    Node *nxt = cur->forward[i].load(std::memory_order_acquire);
                    while (nxt != nullptr && !comp_(key, nxt->key))
                    {
                        cur = nxt;
                        nxt = cur->forward[i].load(std::memory_order_acquire);
                    }
                    preds[i] = cur;
                    succs[i] = nxt;
                    new_node->forward[i].store(nxt, std::memory_order_relaxed);
                }
            }

            // ✅ 版本链记账：level-0 链接成功后，同 key 版本运行的"最新版本"
            // 要么被我们取代（seq 更大时），要么我们立即作废（seq 更小时）。
            // 并发插入都在同一位置 CAS，胜者的搜索视图在链接瞬间仍然有效。
            if (newest_same_key != nullptr)
                return (seq > newest_seq) ? newest_same_key->value : val_raw;
            return std::nullopt;
        }
    }
};