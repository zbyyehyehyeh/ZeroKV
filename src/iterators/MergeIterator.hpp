#pragma once
#include <cstdint>
#include <memory>
#include <queue>
#include <vector>
#include "KVStoreIterator.hpp"

// =====================================================================
// 多路归并迭代器（改动②：改为基于统一迭代器接口的多态归并）
// 同时接受 MemTable / Immutable / SSTable 迭代器，
// 小顶堆排序：key 小者先出；同 key 时 seq 大者先出（新版本优先）。
// 归并堆的版本序保证：无论版本来自哪一层，取 seq 最大的为准。
// =====================================================================
template <typename KeyType>
class MergeIterator
{
public:
    struct Entry
    {
        KeyType key;
        uint64_t seq;
        DataHandle handle;
        int source_id;
        std::unique_ptr<KVStoreIterator<KeyType>> iter;

        bool operator<(const Entry &other) const
        {
            if (key != other.key) return key > other.key;   // 小顶堆：key 小者先出
            return seq < other.seq;                         // 同 key：seq 大者先出
        }

        Entry() = default;
        Entry(Entry&&) noexcept = default;
        Entry& operator=(Entry&&) noexcept = default;
        Entry(const Entry&) = delete;
        Entry& operator=(const Entry&) = delete;
    };

    void AddSource(std::unique_ptr<KVStoreIterator<KeyType>> iter, int source_id)
    {
        if (iter && iter->Valid())
        {
            Entry e;
            e.key = iter->key();
            e.seq = iter->seq();
            e.handle = iter->handle();
            e.source_id = source_id;
            e.iter = std::move(iter);
            heap_.push(std::move(e));
        }
    }

    // ✅ 清空堆，用于 Seek 重置
    void Clear() {
        while (!heap_.empty()) heap_.pop();
    }

    bool Valid() const noexcept { return !heap_.empty(); }
    KeyType key() const noexcept { return heap_.top().key; }
    uint64_t seq() const noexcept { return heap_.top().seq; }
    DataHandle value() const noexcept { return heap_.top().handle; }

    void Next()
    {
        if (heap_.empty()) return;
        Entry top = std::move(const_cast<Entry&>(heap_.top()));
        heap_.pop();
        top.iter->Next();
        if (top.iter->Valid()) {
            top.key = top.iter->key();
            top.seq = top.iter->seq();
            top.handle = top.iter->handle();
            heap_.push(std::move(top));
        }
    }

    DataHandle ConsumeHandle() const { return value(); }

private:
    std::priority_queue<Entry> heap_;
};