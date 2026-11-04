#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "DataHandle.h"
#include "LockFreeSkipList.hpp"
#include "MergeIterator.hpp"
#include "MemTableIterator.hpp"
#include "PinnableSlice.hpp"
#include "SSTable.hpp"
#include "ValueLog.hpp"

namespace kvstore {

// =====================================================================
// DBIterator：合并 MemTable + Immutable + L0 + L1 四类源。
// 关键修复：归并堆每个源同时只保留一条，同 key 的连续版本（版本链 /
// 多版本 SST）可能来自同一源的后续条目。因此每次定位都用
// AdvanceToNextValidKey() 把当前 key 的全部版本排空、取 seq 最大者；
// 最新版本是墓碑则整个 key 跳过。不再依赖"堆顶即最新版本"的假设。
// =====================================================================
class DBIterator {
public:
    using MemIter = MemTableIterator<uint64_t, std::less<uint64_t>>;

    DBIterator(std::shared_ptr<LockFreeSkipList<uint64_t>> active_mem,
               std::vector<std::shared_ptr<LockFreeSkipList<uint64_t>>> imm_mems,
               std::vector<std::shared_ptr<SSTReader<uint64_t>>> l0_ssts,
               std::vector<std::shared_ptr<SSTReader<uint64_t>>> l1_ssts,
               ValueLog* vlog)
        : vlog_(vlog)
    {
        active_mem_ = active_mem;
        imm_mems_ = imm_mems;
        l0_ssts_ = l0_ssts;
        l1_ssts_ = l1_ssts;

        BuildMergeFromStart();
        AdvanceToNextValidKey();
    }

    bool Valid() const { return valid_; }

    void Seek(uint64_t target_key) {
        merger_.Clear();

        if (active_mem_) {
            auto it = std::make_unique<MemIter>(active_mem_->GetHead(), active_mem_.get());
            it->Seek(target_key);
            merger_.AddSource(std::move(it), 0);
        }
        // Imm 从新到旧（后加入的 MemTable 更新）
        for (auto it = imm_mems_.rbegin(); it != imm_mems_.rend(); ++it) {
            auto mi = std::make_unique<MemIter>((*it)->GetHead(), it->get());
            mi->Seek(target_key);
            merger_.AddSource(std::move(mi), 0);
        }
        // L0 从新到旧 (Reverse)
        for (auto it = l0_ssts_.rbegin(); it != l0_ssts_.rend(); ++it) {
            auto si = (*it)->NewIterator();
            si->Seek(target_key);
            merger_.AddSource(std::move(si), 0);
        }
        // L1 从旧到新 (Forward)
        for (auto& sst : l1_ssts_) {
            auto si = sst->NewIterator();
            si->Seek(target_key);
            merger_.AddSource(std::move(si), 0);
        }

        AdvanceToNextValidKey();
    }

    void Next() {
        if (valid_) AdvanceToNextValidKey();
    }

    uint64_t key() const { return current_key_; }

    bool GetValue(std::string& value) {
        if (!valid_) return false;
        DataHandle dh = current_handle_;
        if (dh.is_inline()) {
            char buf[8] = {0};
            dh.copy_inline_to(buf);
            value.assign(buf, dh.size());
            return true;
        } else if (dh.is_vlog()) {
            PinnableSlice slice;
            if (vlog_->Read(dh.vlog_file_id(), dh.vlog_offset(), dh.size(), slice)) {
                value.assign(slice.data(), slice.size());
                return true;
            }
        }
        return false;
    }

private:
    void BuildMergeFromStart() {
        if (active_mem_)
            merger_.AddSource(std::make_unique<MemIter>(active_mem_->GetHead(), active_mem_.get()), 0);
        for (auto it = imm_mems_.rbegin(); it != imm_mems_.rend(); ++it)
            merger_.AddSource(std::make_unique<MemIter>((*it)->GetHead(), it->get()), 0);
        for (auto it = l0_ssts_.rbegin(); it != l0_ssts_.rend(); ++it)
            merger_.AddSource((*it)->NewIterator(), 0);
        for (auto& sst : l1_ssts_)
            merger_.AddSource(sst->NewIterator(), 0);
    }

    // ✅ 修复核心：排空当前 key 的全部版本（来自同源后续条目或其他源），
    // 取 seq 最大者；若最新版本是墓碑，该 key 整体跳过。
    void AdvanceToNextValidKey() {
        valid_ = false;
        while (merger_.Valid()) {
            uint64_t k = merger_.key();
            uint64_t best_seq = 0;
            DataHandle best(0);
            bool any = false;
            while (merger_.Valid() && merger_.key() == k) {
                if (!any || merger_.seq() > best_seq) {
                    best_seq = merger_.seq();
                    best = merger_.value();
                    any = true;
                }
                merger_.Next();
            }
            if (any && !best.is_tombstone()) {
                current_key_ = k;
                current_handle_ = best;
                valid_ = true;
                return;
            }
            // 最新版本是墓碑 → 继续下一个 key
        }
    }

    MergeIterator<uint64_t> merger_;
    ValueLog* vlog_;

    // 持有引用，防止生命周期问题
    std::shared_ptr<LockFreeSkipList<uint64_t>> active_mem_;
    std::vector<std::shared_ptr<LockFreeSkipList<uint64_t>>> imm_mems_;
    std::vector<std::shared_ptr<SSTReader<uint64_t>>> l0_ssts_;
    std::vector<std::shared_ptr<SSTReader<uint64_t>>> l1_ssts_;

    // ✅ 当前定位到的有效版本（不再依赖堆顶）
    uint64_t current_key_ = 0;
    DataHandle current_handle_{0};
    bool valid_ = false;
};

} // namespace kvstore