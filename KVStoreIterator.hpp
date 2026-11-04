#pragma once
#include <cstdint>
#include "DataHandle.h"

// =====================================================================
// 统一迭代器接口（改动②的基础设施）
// MemTable / Immutable / SSTable 的迭代器都实现该接口，
// MergeIterator（归并堆）与 DBIterator（范围扫描）基于多态接口工作，
// 从而让范围扫描同时覆盖"未落盘的 MemTable 数据"与"已落盘的 SST 数据"。
// =====================================================================
template <typename KeyType>
class KVStoreIterator
{
public:
    virtual ~KVStoreIterator() = default;
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual void Seek(const KeyType& target) = 0;
    virtual KeyType key() const = 0;
    virtual uint64_t seq() const = 0;
    virtual DataHandle handle() const = 0;
};