#pragma once
#include <cstdint>
#include <functional>
#include "KVStoreIterator.hpp"
#include "LockFreeSkipList.hpp"
#include "DataHandle.h"

// =====================================================================
// MemTable 迭代器（实现统一迭代器接口，供 MergeIterator 归并）
// 注意：Flush 期间该 MemTable 不再接收写入，遍历安全；
//       value/seq 是链接后不可变的普通字段（追加式版本链设计）。
// =====================================================================
template <typename KeyType, typename Compare = std::less<KeyType>>
class MemTableIterator : public KVStoreIterator<KeyType>
{
public:
    using Node = typename LockFreeSkipList<KeyType, Compare>::NodeType;
    using SkipListType = LockFreeSkipList<KeyType, Compare>;

    explicit MemTableIterator(Node *head, const SkipListType *sl = nullptr)
        : current_(head->forward[0].load(std::memory_order_acquire)), sl_(sl), head_(head) {}

    bool Valid() const noexcept override { return current_ != nullptr; }

    void Next() noexcept override
    {
        if (current_)
            current_ = current_->forward[0].load(std::memory_order_acquire);
    }

    void Seek(const KeyType& target) override
    {
        if (sl_) {
            current_ = sl_->Seek(target);
        } else {
            // Fallback 如果没传入 skiplist 指针，只能线性扫描（不推荐）
            current_ = head_->forward[0].load(std::memory_order_acquire);
            while (current_ && Compare()(current_->key, target)) {
                current_ = current_->forward[0].load(std::memory_order_acquire);
            }
        }
    }

    KeyType key() const override { return current_->key; }

    // ✅ 追加式版本链：value/seq 直接读普通字段（不可变，无撕裂）
    DataHandle handle() const override { return DataHandle(current_->value); }

    uint64_t seq() const override { return current_->seq; }

private:
    Node *current_;
    const SkipListType *sl_;
    Node *head_;
};