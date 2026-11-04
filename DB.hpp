#pragma once
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <climits>
#include <optional>
#include <set>
#include <unordered_set>
#include <functional>
#include <unordered_map>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "LockFreeSkipList.hpp"
#include "SSTable.hpp"
#include "WAL.hpp"
#include "ThreadPool.hpp"
#include "MergeIterator.hpp"
#include "WriteController.hpp"
#include "ValueLog.hpp"
#include "ValueLogGC.hpp"
#include "CompactionTask.hpp"
#include "MemTableIterator.hpp"
#include "MemPool.hpp"
#include "DataHandle.h"
#include "DBIterator.hpp"

namespace kvstore
{
    using MemTable = LockFreeSkipList<uint64_t, std::less<uint64_t>>;
    using NodePool = TlsMemPoolManager<typename MemTable::NodeType>;
    using VLogRewriteMap = std::unordered_map<uint64_t, uint64_t>;

    struct DBSSTMeta
    {
        uint64_t file_id;
        std::string filepath;
        uint64_t min_key, max_key, max_seq, file_size;
        int level;
        std::shared_ptr<SSTReader<uint64_t>> reader;
    };

    CompactionOutput CompactionTask::Execute(
        const CompactionInput &input,
        const std::string &dir,
        std::atomic<uint64_t> &next_file_id,
        size_t target_sst_size)
    {
        CompactionOutput output;
        MergeIterator<uint64_t> merger;

        int sid = 0;
        for (auto &r : input.sources)
        {
            if (r)
                merger.AddSource(r->NewIterator(), sid++);
        }

        std::optional<SSTWriter<uint64_t>> writer;
        uint64_t cur_fid = 0;
        size_t cur_size = 0;
        uint64_t cur_max_seq = 0;

        auto finish_current = [&]()
        {
            if (writer && writer->GetFileSize() > 0)
            {
                writer->Finish();
                DBSSTMeta meta;
                meta.file_id = cur_fid;
                meta.filepath = dir + "/sst_" + std::to_string(cur_fid) + ".sst";
                meta.min_key = writer->GetMinKey();
                meta.max_key = writer->GetMaxKey();
                meta.max_seq = cur_max_seq;
                meta.file_size = writer->GetFileSize();
                meta.level = input.target_level;
                // ✅ BlockCache 命名空间：新 SST 的 reader 与所属 DB 实例共享缓存空间
                meta.reader = std::make_shared<SSTReader<uint64_t>>(meta.filepath, cur_fid, input.cache_ns);
                output.new_ssts.push_back(std::move(meta));
            }
        };

        while (merger.Valid())
        {
            // ✅ 修复：先排空"当前 key 的全部版本"再决策。归并堆每源同时只有
            // 一条，同 key 的连续版本（版本链 / 多版本 SST）可能来自同一源的
            // 后续条目，旧实现按堆顶逐个判断会取到旧版本或漏掉墓碑。
            uint64_t k = merger.key();
            uint64_t best_seq = 0;
            DataHandle best_dh(0);
            bool any = false;

            while (merger.Valid() && merger.key() == k)
            {
                uint64_t s = merger.seq();
                DataHandle dh = merger.value();
                merger.Next();

                if (!any || s > best_seq)
                {
                    // 之前的最佳版本被取代 → VLog 可回收记账
                    if (any && input.is_bottom_level && input.handle_obsolete)
                        input.handle_obsolete(k, best_dh.raw);
                    best_seq = s;
                    best_dh = dh;
                    any = true;
                }
                else
                {
                    // 本次版本更旧 → VLog 可回收记账
                    if (input.is_bottom_level && input.handle_obsolete)
                        input.handle_obsolete(k, dh.raw);
                }
            }

            if (!any)
                continue;

            if (input.is_bottom_level)
            {
                // 最新版本是墓碑：该 key 全部作废
                if (best_dh.is_tombstone())
                {
                    if (input.handle_obsolete)
                        input.handle_obsolete(k, best_dh.raw);
                    continue;
                }
            }

            if (input.handle_rewriter)
            {
                uint64_t new_raw = input.handle_rewriter(k, best_dh.raw);
                if (new_raw != best_dh.raw)
                    best_dh = DataHandle(new_raw);
            }

            if (!writer || cur_size >= target_sst_size)
            {
                finish_current();
                cur_fid = next_file_id.fetch_add(1, std::memory_order_acq_rel);
                std::string path = dir + "/sst_" + std::to_string(cur_fid) + ".sst";
                writer.emplace(path);
                cur_size = 0;
                cur_max_seq = 0;
            }

            writer->Add(k, best_dh, best_seq);
            cur_size += 24;
            cur_max_seq = std::max(cur_max_seq, best_seq);
        }
        finish_current();
        return output;
    }

    class DB
    {
    public:
        explicit DB(const std::string &dir, size_t block_cache_blocks = 8192) : dir_(dir), cache_ns_(NextCacheNs())
        {
            std::filesystem::create_directories(dir);
            BlockCache::instance(block_cache_blocks);
            NodePool::instance().InitThreadLocalPool();

            bg_pool_ = std::make_unique<ThreadPool>(4, []
                                                    { NodePool::instance().InitThreadLocalPool(); }, []
                                                    { NodePool::instance().ReleaseThreadLocalPool(); });

            memtable_ = std::make_shared<MemTable>();
            vlog_ = std::make_unique<ValueLog>(dir + "/vlog");
            vlog_gc_ = std::make_unique<ValueLogGC>(vlog_.get());

            vlog_->RecoverAndTruncate();
            uint32_t valid_vlog_file = vlog_->GetCurrentFileId();
            uint32_t valid_vlog_size = vlog_->GetCurrentValidSize();

            uint64_t manifest_max_seq = LoadManifest();

            std::string wal_path = dir_ + "/wal.log";
            uint64_t wal_max_seq = WAL::Recover(wal_path,
                                                [this, valid_vlog_file, valid_vlog_size](uint64_t seq, uint64_t k, const char *v, uint32_t vlen, WALRecordType type)
                                                {
                                                    if (vlen == sizeof(uint64_t))
                                                    {
                                                        DataHandle dh;
                                                        std::memcpy(&dh.raw, v, sizeof(uint64_t));
                                                        if (dh.is_vlog())
                                                        {
                                                            uint32_t fid = dh.vlog_file_id();
                                                            uint32_t off = dh.vlog_offset();
                                                            uint32_t len = dh.size();
                                                            if (fid > valid_vlog_file || (fid == valid_vlog_file && (off + sizeof(ValueLog::VLogRecordHeader) + sizeof(uint64_t) + len) > valid_vlog_size))
                                                                return;
                                                        }
                                                        if (type == WALRecordType::PUT || type == WALRecordType::DELETE)
                                                        {
                                                            // ✅ 改动③配套：Put 返回被取代的旧版本，恢复期间忽略返回值
                                                            //（discardable 是内存态统计，崩溃后从零重建，GC 扫描时由 checker 兜底）
                                                            memtable_->Put(k, dh, seq);
                                                            memtable_bytes_.fetch_add(24, std::memory_order_relaxed);
                                                        }
                                                    }
                                                });

            next_seq_.store(std::max(manifest_max_seq, wal_max_seq) + 1, std::memory_order_release);
            wal_ = std::make_unique<WAL>(wal_path);

            pending_rewrite_map_ = std::make_shared<VLogRewriteMap>();

            UpdateWriteControllerState();
            MaybeScheduleCompaction();
        }

        ~DB() { Shutdown(); }

        ValueLog *GetVLog() { return vlog_.get(); }
        ValueLogGC *GetVLogGC() { return vlog_gc_.get(); }

        void InjectRewriteMap(VLogRewriteMap &&map)
        {
            std::lock_guard<std::mutex> lk(rewrite_mtx_);
            if (!pending_rewrite_map_)
            {
                pending_rewrite_map_ = std::make_shared<VLogRewriteMap>(std::move(map));
            }
            else
            {
                for (auto &[k, v] : map)
                {
                    (*pending_rewrite_map_)[k] = v;
                }
            }
        }

        bool GetLatestHandleRaw(uint64_t key, uint64_t &out_raw)
        {
            uint64_t best_seq = 0;
            bool found = false;
            bool is_tombstone = false;

            auto check = [&](uint64_t raw, uint64_t seq)
            {
                if (seq > best_seq)
                {
                    best_seq = seq;
                    out_raw = raw;
                    found = true;
                    is_tombstone = DataHandle(raw).is_tombstone();
                }
            };

            {
                std::shared_lock<std::shared_mutex> rl(memtable_mtx_);
                if (memtable_)
                {
                    uint64_t s = 0, r = 0;
                    if (memtable_->GetHandle(key, r, s))
                        check(r, s);
                }
            }
            {
                std::shared_lock<std::shared_mutex> rl(imm_mtx_);
                for (auto it = imm_memtables_.rbegin(); it != imm_memtables_.rend(); ++it)
                {
                    uint64_t s = 0, r = 0;
                    if ((*it)->GetHandle(key, r, s))
                        check(r, s);
                }
            }
            {
                std::shared_lock<std::shared_mutex> rl(sst_mtx_);
                for (auto it = l0_ssts_.rbegin(); it != l0_ssts_.rend(); ++it)
                {
                    if (key >= it->min_key && key <= it->max_key && it->reader)
                    {
                        PinnableSlice t;
                        uint64_t s = 0;
                        if (it->reader->Get(key, t, s) && t.size() == 8)
                        {
                            uint64_t r;
                            std::memcpy(&r, t.data(), 8);
                            check(r, s);
                        }
                    }
                }
                for (auto &sst : l1_ssts_)
                {
                    if (key >= sst.min_key && key <= sst.max_key && sst.reader)
                    {
                        PinnableSlice t;
                        uint64_t s = 0;
                        if (sst.reader->Get(key, t, s) && t.size() == 8)
                        {
                            uint64_t r;
                            std::memcpy(&r, t.data(), 8);
                            check(r, s);
                        }
                    }
                }
            }
            return found && !is_tombstone;
        }

        // ========================= PUT ==========================
        void Put(uint64_t key, const char *value, uint32_t value_len)
        {
            std::shared_lock<std::shared_mutex> pause_lk(gc_pause_mtx_);

            WriteController::instance().ApplyBackpressure();
            uint64_t seq = next_seq_.fetch_add(1, std::memory_order_acq_rel);
            DataHandle dh;

            if (value_len <= 5)
            {
                dh = DataHandle::make_inline(DataType::RAW, value, value_len);
            }
            else
            {
                uint32_t fid, off;
                vlog_->Append(key, value, value_len, fid, off);
                dh = DataHandle::make_vlog(DataType::RAW, fid, off, value_len);
            }
            wal_->Append(seq, key, reinterpret_cast<const char *>(&dh.raw), sizeof(uint64_t), WALRecordType::PUT);

            std::optional<uint64_t> old_raw = memtable_->Put(key, dh, seq);
            if (old_raw.has_value())
            {
                DataHandle old_dh(*old_raw);
                if (old_dh.is_vlog())
                {
                    uint32_t old_total_len = sizeof(ValueLog::VLogRecordHeader) + sizeof(uint64_t) + old_dh.size();
                    vlog_->AddDiscardableSize(old_dh.vlog_file_id(), old_total_len);
                }
            }
            CheckFlush();
        }

        // ========================= DELETE ==========================
        void Delete(uint64_t key)
        {
            std::shared_lock<std::shared_mutex> pause_lk(gc_pause_mtx_);

            WriteController::instance().ApplyBackpressure();
            uint64_t seq = next_seq_.fetch_add(1, std::memory_order_acq_rel);

            DataHandle dh = DataHandle::make_tombstone();
            wal_->Append(seq, key, reinterpret_cast<const char *>(&dh.raw), sizeof(uint64_t), WALRecordType::DELETE);

            std::optional<uint64_t> old_raw = memtable_->Put(key, dh, seq);
            if (old_raw.has_value())
            {
                DataHandle old_dh(*old_raw);
                if (old_dh.is_vlog())
                {
                    uint32_t old_total_len = sizeof(ValueLog::VLogRecordHeader) + sizeof(uint64_t) + old_dh.size();
                    vlog_->AddDiscardableSize(old_dh.vlog_file_id(), old_total_len);
                }
            }
            CheckFlush();
        }

        // ========================= GET ==========================
        bool Get(uint64_t key, std::string &value)
        {
            PinnableSlice pval;
            if (GetZeroCopy(key, pval))
            {
                value.assign(pval.data(), pval.size());
                return true;
            }
            return false;
        }

        bool GetZeroCopy(uint64_t key, PinnableSlice &out_val)
        {
            uint64_t seq;
            return GetZeroCopyInternal(key, out_val, seq);
        }

        void SyncWAL()
        {
            if (wal_)
                wal_->Sync();
        }

        void WaitForBackgroundJobs()
        {
            std::unique_lock<std::mutex> lk(bg_mtx_);
            bg_cv_.wait(lk, [this]
                        { return pending_bg_jobs_.load(std::memory_order_acquire) == 0; });
        }

        // ========================= GC ==========================
        bool RunGC(bool force = false)
        {
            uint32_t target_fid;
            if (force)
            {
                target_fid = vlog_->PickGCFile(0.0);
            }
            else
            {
                target_fid = vlog_->PickGCFile(GC_DISCARDABLE_THRESHOLD);
            }

            if (target_fid == UINT32_MAX)
                return false;

            std::unique_lock<std::shared_mutex> pause_lk(gc_pause_mtx_);

            FlushActiveMemTable();
            WaitForBackgroundJobs();

            auto checker = [this](uint64_t key, uint64_t &out_raw) -> bool
            {
                return this->GetLatestHandleRaw(key, out_raw);
            };

            auto rewrite_map = vlog_gc_->RunOnce(target_fid, checker);

            if (!rewrite_map.empty())
            {
                InjectRewriteMap(std::move(rewrite_map));
                ForceGlobalCompactionInternal();
            }

            vlog_->RemoveFile(target_fid);

            return true;
        }

        int RunFullGC()
        {
            // ✅ FIX: 先强制轮转 VLog 文件，让当前活跃文件变成可 GC 的旧文件
            // 根因：所有数据可能都在同一个 VLog 文件中（未达 900MB 轮转阈值），
            // 而 PickGCFile 会跳过 current_file_id_，导致永远找不到可回收的文件
            vlog_->RollToNewFile();

            int rounds = 0;
            while (rounds < MAX_GC_ROUNDS_PER_BATCH && RunGC(true))
            {
                rounds++;
            }
            return rounds;
        }

        void BackgroundGCLoop(std::atomic<bool> &shutdown_flag)
        {
            while (!shutdown_flag.load(std::memory_order_relaxed))
            {
                bool did_gc = RunGC(false);
                if (!did_gc)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }
        }

        // ========================= SHUTDOWN ==========================
        void Shutdown()
        {
            if (shutdown_.exchange(true))
                return;
            if (wal_)
                wal_->Sync();

            FlushActiveMemTable();
            WaitForBackgroundJobs();
            SaveManifest();
            BlockCache::instance().clear();
            wal_.reset();
            vlog_.reset();
            bg_pool_.reset();
            NodePool::instance().ReleaseThreadLocalPool();
        }

        // ========================= ITERATOR ==========================
        std::unique_ptr<DBIterator> NewIterator()
        {
            std::shared_lock<std::shared_mutex> mem_lk(memtable_mtx_);
            std::shared_lock<std::shared_mutex> imm_lk(imm_mtx_);
            std::shared_lock<std::shared_mutex> sst_lk(sst_mtx_);

            auto active_mem = memtable_;
            auto imm_mems = imm_memtables_;

            std::vector<std::shared_ptr<SSTReader<uint64_t>>> l0_readers;
            l0_readers.reserve(l0_ssts_.size());
            for (const auto &meta : l0_ssts_)
            {
                l0_readers.push_back(meta.reader);
            }

            std::vector<std::shared_ptr<SSTReader<uint64_t>>> l1_readers;
            l1_readers.reserve(l1_ssts_.size());
            for (const auto &meta : l1_ssts_)
            {
                l1_readers.push_back(meta.reader);
            }

            return std::make_unique<DBIterator>(
                active_mem,
                imm_mems,
                std::move(l0_readers),
                std::move(l1_readers),
                vlog_.get());
        }

        void ForceGlobalCompaction()
        {
            std::unique_lock<std::shared_mutex> pause_lk(gc_pause_mtx_);
            ForceGlobalCompactionInternal();
        }

    private:
        static constexpr size_t MEMTABLE_FLUSH_THRESHOLD = 16 * 1024 * 1024;
        static constexpr size_t L0_COMPACTION_TRIGGER = 4;
        static constexpr size_t TARGET_SST_SIZE = 16 * 1024 * 1024;
        static constexpr int BOTTOM_LEVEL = 1;
        static constexpr int MAX_GC_ROUNDS_PER_BATCH = 5;
        static constexpr double GC_DISCARDABLE_THRESHOLD = 0.3;

        std::string dir_;
        uint64_t cache_ns_;   // ✅ BlockCache 命名空间（每实例唯一）

        std::atomic<bool> shutdown_{false};
        std::atomic<uint64_t> next_seq_{1};
        std::shared_ptr<MemTable> memtable_;
        std::atomic<size_t> memtable_bytes_{0};
        mutable std::shared_mutex memtable_mtx_;
        std::vector<std::shared_ptr<MemTable>> imm_memtables_;
        mutable std::shared_mutex imm_mtx_;
        std::vector<DBSSTMeta> l0_ssts_, l1_ssts_;
        mutable std::shared_mutex sst_mtx_;
        std::unique_ptr<WAL> wal_;
        std::unique_ptr<ValueLog> vlog_;
        std::unique_ptr<ValueLogGC> vlog_gc_;
        std::unique_ptr<ThreadPool> bg_pool_;
        std::mutex bg_mtx_;
        std::condition_variable bg_cv_;
        std::atomic<int> pending_bg_jobs_{0};
        std::atomic<uint64_t> next_file_id_{1};
        std::atomic<int> active_compactions_{0};
        static constexpr int MAX_CONCURRENT_COMPACTIONS = 2;

        std::shared_ptr<VLogRewriteMap> pending_rewrite_map_;
        mutable std::mutex rewrite_mtx_;

        mutable std::shared_mutex gc_pause_mtx_;

        static uint64_t NextCacheNs()
        {
            static std::atomic<uint64_t> next{1};
            return next.fetch_add(1, std::memory_order_relaxed);
        }

        bool IsMemTableEmpty(MemTable *m) { return m->GetHead()->forward[0].load(std::memory_order_acquire) == nullptr; }

        void UpdateWriteControllerState()
        {
            std::shared_lock<std::shared_mutex> rl(sst_mtx_);
            std::shared_lock<std::shared_mutex> il(imm_mtx_);
            WriteController::instance().UpdateState(static_cast<int>(l0_ssts_.size()), static_cast<int>(imm_memtables_.size()));
        }

        void CheckFlush()
        {
            auto nb = memtable_bytes_.fetch_add(24, std::memory_order_acq_rel) + 24;
            if (nb >= MEMTABLE_FLUSH_THRESHOLD)
                TriggerFlush();
        }

        void FinishBackgroundJob() noexcept
        {
            int prev = pending_bg_jobs_.fetch_sub(1, std::memory_order_acq_rel);
            if (prev == 1)
            {
                std::lock_guard<std::mutex> lk(bg_mtx_);
                bg_cv_.notify_all();
            }
        }

        void TriggerFlush()
        {
            std::shared_ptr<MemTable> old;
            {
                std::unique_lock<std::shared_mutex> wl(memtable_mtx_);
                if (!memtable_ || IsMemTableEmpty(memtable_.get()))
                    return;
                old = memtable_;
                memtable_ = std::make_shared<MemTable>();
                memtable_bytes_.store(0, std::memory_order_release);
            }
            {
                std::unique_lock<std::shared_mutex> il(imm_mtx_);
                imm_memtables_.push_back(old);
            }
            UpdateWriteControllerState();
            pending_bg_jobs_.fetch_add(1, std::memory_order_acq_rel);
            bg_pool_->enqueue([this, old]()
                              { 
                                  try { FlushMemTable(old.get()); } catch(...) {} 
                                  MaybeScheduleCompaction(); 
                                  FinishBackgroundJob(); });
        }

        void FlushActiveMemTable()
        {
            std::shared_ptr<MemTable> old;
            {
                std::unique_lock<std::shared_mutex> wl(memtable_mtx_);
                if (!memtable_ || IsMemTableEmpty(memtable_.get()))
                    return;
                old = memtable_;
                memtable_ = std::make_shared<MemTable>();
                memtable_bytes_.store(0, std::memory_order_release);
            }
            {
                std::unique_lock<std::shared_mutex> il(imm_mtx_);
                imm_memtables_.push_back(old);
            }

            pending_bg_jobs_.fetch_add(1, std::memory_order_acq_rel);
            try
            {
                FlushMemTable(old.get());
            }
            catch (...)
            {
            }
            FinishBackgroundJob();
        }

        void FlushMemTable(MemTable *mem)
        {
            uint64_t id = next_file_id_.fetch_add(1, std::memory_order_acq_rel);
            std::string path = dir_ + "/sst_" + std::to_string(id) + ".sst";
            SSTWriter<uint64_t> w(path);
            MemTableIterator<uint64_t, std::less<uint64_t>> it(mem->GetHead(), mem);
            uint64_t ms = 0;
            while (it.Valid())
            {
                // ✅ 改动②配套：统一迭代器接口后，取句柄用 handle()
                DataHandle dh = it.handle();
                uint64_t s = it.seq();
                w.Add(it.key(), dh, s);
                ms = std::max(ms, s);
                it.Next();
            }
            w.Finish();
            DBSSTMeta meta{id, path, w.GetMinKey(), w.GetMaxKey(), ms, w.GetFileSize(), 0, std::make_shared<SSTReader<uint64_t>>(path, id, cache_ns_)};
            {
                std::unique_lock<std::shared_mutex> wl(sst_mtx_);
                l0_ssts_.push_back(std::move(meta));
            }
            {
                std::unique_lock<std::shared_mutex> wl(imm_mtx_);
                imm_memtables_.erase(std::remove_if(imm_memtables_.begin(), imm_memtables_.end(), [mem](const auto &p)
                                                    { return p.get() == mem; }),
                                     imm_memtables_.end());
            }
            UpdateWriteControllerState();
            if (wal_)
                wal_->Rotate();
        }

        void MaybeScheduleCompaction()
        {
            if (shutdown_.load(std::memory_order_acquire))
                return;
            int l0sz = 0;
            bool l1_overlap = false;
            {
                std::shared_lock<std::shared_mutex> rl(sst_mtx_);
                l0sz = static_cast<int>(l0_ssts_.size());
                for (size_t i = 1; i < l1_ssts_.size(); i++)
                {
                    if (l1_ssts_[i].min_key <= l1_ssts_[i - 1].max_key)
                    {
                        l1_overlap = true;
                        break;
                    }
                }
            }
            bool should_compact = (l0sz >= L0_COMPACTION_TRIGGER) || l1_overlap;
            if (!should_compact)
                return;

            while (active_compactions_.load(std::memory_order_acquire) < MAX_CONCURRENT_COMPACTIONS)
            {
                if (active_compactions_.fetch_add(1, std::memory_order_acq_rel) < MAX_CONCURRENT_COMPACTIONS)
                {
                    pending_bg_jobs_.fetch_add(1, std::memory_order_acq_rel);
                    bg_pool_->enqueue([this]()
                                      {
                        try { DoCompaction(); } catch(const std::exception&e) { std::cerr<<"[Compaction]"<<e.what()<<std::endl; } catch(...) {}
                        active_compactions_.fetch_sub(1, std::memory_order_acq_rel);
                        FinishBackgroundJob(); });
                    break;
                }
                else
                {
                    active_compactions_.fetch_sub(1, std::memory_order_acq_rel);
                    break;
                }
            }
        }

    private:
        void ForceGlobalCompactionInternal()
        {
            std::shared_ptr<VLogRewriteMap> local_map;
            {
                std::lock_guard<std::mutex> lk(rewrite_mtx_);
                local_map = pending_rewrite_map_;
                pending_rewrite_map_ = std::make_shared<VLogRewriteMap>();
            }

            FlushActiveMemTable();
            WaitForBackgroundJobs();

            std::vector<DBSSTMeta> sources;
            {
                std::unique_lock<std::shared_mutex> wl(sst_mtx_);
                sources = l0_ssts_;
                sources.insert(sources.end(), l1_ssts_.begin(), l1_ssts_.end());

                if (sources.empty())
                {
                    return;
                }
            }

            std::vector<std::shared_ptr<SSTReader<uint64_t>>> readers;
            readers.reserve(sources.size());
            for (auto &s : sources)
                readers.push_back(s.reader);

            CompactionInput input;
            input.sources = std::move(readers);
            input.target_level = 1;
            input.is_bottom_level = true;
            input.cache_ns = cache_ns_;   // ✅ BlockCache 命名空间

            // ✅ 改动①：跨层覆盖的旧版本在此统一记 discardable（与写路径版本链记账互补且不重复）
            input.handle_obsolete = [this](uint64_t, uint64_t old_raw)
            {
                DataHandle old_dh(old_raw);
                if (old_dh.is_vlog())
                {
                    uint32_t old_total = sizeof(ValueLog::VLogRecordHeader) + sizeof(uint64_t) + old_dh.size();
                    vlog_->AddDiscardableSize(old_dh.vlog_file_id(), old_total);
                }
            };

            if (local_map && !local_map->empty())
            {
                input.handle_rewriter = [local_map](uint64_t key, uint64_t old_raw) -> uint64_t
                {
                    auto it = local_map->find(key);
                    return (it != local_map->end()) ? it->second : old_raw;
                };
            }

            auto output = CompactionTask::Execute(input, dir_, next_file_id_, TARGET_SST_SIZE);

            std::vector<std::string> files_to_delete;
            {
                std::unique_lock<std::shared_mutex> wl(sst_mtx_);

                std::unordered_set<uint64_t> src_ids;
                src_ids.reserve(sources.size());
                for (const auto &s : sources)
                    src_ids.insert(s.file_id);

                l0_ssts_.erase(std::remove_if(l0_ssts_.begin(), l0_ssts_.end(),
                                              [&](const DBSSTMeta &m)
                                              { return src_ids.count(m.file_id) > 0; }),
                               l0_ssts_.end());

                l1_ssts_.erase(std::remove_if(l1_ssts_.begin(), l1_ssts_.end(),
                                              [&](const DBSSTMeta &m)
                                              { return src_ids.count(m.file_id) > 0; }),
                               l1_ssts_.end());

                for (auto &s : output.new_ssts)
                    l1_ssts_.push_back(std::move(s));

                std::sort(l1_ssts_.begin(), l1_ssts_.end(),
                          [](const auto &a, const auto &b)
                          { return a.min_key < b.min_key; });

                files_to_delete.reserve(sources.size());
                for (const auto &s : sources)
                    files_to_delete.push_back(s.filepath);

                SaveManifestInternal();
            }

            for (const auto &f : files_to_delete)
            {
                std::error_code ec;
                std::filesystem::remove(f, ec);
            }

            UpdateWriteControllerState();
        }

        void DoCompaction()
        {
            std::vector<DBSSTMeta> sources;
            uint64_t min_key = UINT64_MAX, max_key = 0;
            int target_level = 1;
            bool is_l1_internal = false;

            {
                std::unique_lock<std::shared_mutex> wl(sst_mtx_);
                if (l0_ssts_.size() >= L0_COMPACTION_TRIGGER)
                {
                    sources = l0_ssts_;
                    for (auto &s : sources)
                    {
                        min_key = std::min(min_key, s.min_key);
                        max_key = std::max(max_key, s.max_key);
                    }
                }
                else
                {
                    for (size_t i = 1; i < l1_ssts_.size(); i++)
                    {
                        if (l1_ssts_[i].min_key <= l1_ssts_[i - 1].max_key)
                        {
                            sources.push_back(l1_ssts_[i - 1]);
                            sources.push_back(l1_ssts_[i]);
                            min_key = std::min(l1_ssts_[i - 1].min_key, l1_ssts_[i].min_key);
                            max_key = std::max(l1_ssts_[i - 1].max_key, l1_ssts_[i].max_key);
                            is_l1_internal = true;
                            break;
                        }
                    }
                    if (sources.empty())
                        return;
                }
            }

            std::vector<DBSSTMeta> l1_sources, l1_remaining;
            if (!is_l1_internal)
            {
                std::shared_lock<std::shared_mutex> rl(sst_mtx_);
                for (auto &sst : l1_ssts_)
                {
                    if (sst.max_key >= min_key && sst.min_key <= max_key)
                        l1_sources.push_back(sst);
                    else
                        l1_remaining.push_back(sst);
                }
            }

            std::vector<std::shared_ptr<SSTReader<uint64_t>>> readers;
            if (!is_l1_internal)
                for (auto &s : l1_sources)
                    readers.push_back(s.reader);
            for (auto &s : sources)
                readers.push_back(s.reader);

            CompactionInput input;
            input.sources = std::move(readers);
            input.target_level = target_level;
            input.is_bottom_level = (target_level == BOTTOM_LEVEL);
            input.cache_ns = cache_ns_;   // ✅ BlockCache 命名空间

            // ✅ 改动①：跨层覆盖的旧版本统一记账（写路径只覆盖同一 MemTable 内的覆盖）
            input.handle_obsolete = [this](uint64_t, uint64_t old_raw)
            {
                DataHandle old_dh(old_raw);
                if (old_dh.is_vlog())
                {
                    uint32_t old_total = sizeof(ValueLog::VLogRecordHeader) + sizeof(uint64_t) + old_dh.size();
                    vlog_->AddDiscardableSize(old_dh.vlog_file_id(), old_total);
                }
            };

            std::shared_ptr<VLogRewriteMap> local_map;
            {
                std::lock_guard<std::mutex> lk(rewrite_mtx_);
                local_map = pending_rewrite_map_;
            }

            if (local_map && !local_map->empty())
            {
                input.handle_rewriter = [local_map](uint64_t key, uint64_t old_raw) -> uint64_t
                {
                    auto it = local_map->find(key);
                    return (it != local_map->end()) ? it->second : old_raw;
                };
            }

            auto output = CompactionTask::Execute(input, dir_, next_file_id_, TARGET_SST_SIZE);

            std::vector<std::string> files_to_delete;
            {
                std::unique_lock<std::shared_mutex> wl(sst_mtx_);
                std::set<uint64_t> src_ids;
                for (auto &s : sources)
                    src_ids.insert(s.file_id);

                if (is_l1_internal)
                {
                    l1_ssts_.erase(std::remove_if(l1_ssts_.begin(), l1_ssts_.end(), [&](const DBSSTMeta &m)
                                                  { return src_ids.count(m.file_id) > 0; }),
                                   l1_ssts_.end());
                }
                else
                {
                    l0_ssts_.erase(std::remove_if(l0_ssts_.begin(), l0_ssts_.end(), [&](const DBSSTMeta &m)
                                                  { return src_ids.count(m.file_id) > 0; }),
                                   l0_ssts_.end());
                    l1_ssts_ = std::move(l1_remaining);
                }

                for (auto &s : output.new_ssts)
                    l1_ssts_.push_back(std::move(s));
                std::sort(l1_ssts_.begin(), l1_ssts_.end(), [](const auto &a, const auto &b)
                          { return a.min_key < b.min_key; });

                for (auto &s : sources)
                    files_to_delete.push_back(s.filepath);
                if (!is_l1_internal)
                    for (auto &s : l1_sources)
                        files_to_delete.push_back(s.filepath);

                SaveManifestInternal();
            }

            for (auto &f : files_to_delete)
            {
                std::error_code ec;
                std::filesystem::remove(f, ec);
            }
            UpdateWriteControllerState();
            MaybeScheduleCompaction();
        }

        bool GetZeroCopyInternal(uint64_t key, PinnableSlice &out_val, uint64_t &out_seq)
        {
            uint64_t best_seq = 0;
            bool found = false;
            uint64_t handle_raw = 0;
            bool is_tombstone = false;

            auto check = [&](uint64_t raw, uint64_t seq)
            {
                if (seq > best_seq)
                {
                    best_seq = seq;
                    handle_raw = raw;
                    found = true;
                    is_tombstone = DataHandle(raw).is_tombstone();
                }
            };

            {
                std::shared_lock<std::shared_mutex> rl(memtable_mtx_);
                if (memtable_)
                {
                    uint64_t s = 0, r = 0;
                    if (memtable_->GetHandle(key, r, s))
                        check(r, s);
                }
            }

            {
                std::shared_lock<std::shared_mutex> rl(imm_mtx_);
                for (auto it = imm_memtables_.rbegin(); it != imm_memtables_.rend(); ++it)
                {
                    uint64_t s = 0, r = 0;
                    if ((*it)->GetHandle(key, r, s))
                        check(r, s);
                }
            }

            {
                std::shared_lock<std::shared_mutex> rl(sst_mtx_);
                for (auto it = l0_ssts_.rbegin(); it != l0_ssts_.rend(); ++it)
                {
                    if (key >= it->min_key && key <= it->max_key && it->reader)
                    {
                        PinnableSlice t;
                        uint64_t s = 0;
                        if (it->reader->Get(key, t, s) && t.size() == 8)
                        {
                            uint64_t r;
                            std::memcpy(&r, t.data(), 8);
                            check(r, s);
                        }
                    }
                }

                for (auto &sst : l1_ssts_)
                {
                    if (key >= sst.min_key && key <= sst.max_key && sst.reader)
                    {
                        PinnableSlice t;
                        uint64_t s = 0;
                        if (sst.reader->Get(key, t, s) && t.size() == 8)
                        {
                            uint64_t r;
                            std::memcpy(&r, t.data(), 8);
                            check(r, s);
                        }
                    }
                }
            }

            if (found)
            {
                out_seq = best_seq;
                if (is_tombstone)
                    return false;

                DataHandle dh(handle_raw);
                if (dh.is_inline())
                {
                    char b[8] = {0};
                    dh.copy_inline_to(b);
                    out_val.assign(b, dh.size());
                    return true;
                }
                else if (dh.is_vlog())
                {
                    bool ok = vlog_->Read(dh.vlog_file_id(), dh.vlog_offset(), dh.size(), out_val);
                    if (!ok)
                    {
                        static std::atomic<int> fail_count{0};
                        if (fail_count.fetch_add(1) < 5)
                        {
                            std::cerr << "[DIAG] VLog Read FAILED: key=" << key
                                      << " fid=" << dh.vlog_file_id()
                                      << " off=" << dh.vlog_offset()
                                      << " len=" << dh.size()
                                      << " handle_raw=0x" << std::hex << handle_raw << std::dec
                                      << std::endl;
                        }
                    }
                    return ok;
                }
            }
            return false;
        }

        uint64_t LoadManifest()
        {
            std::string path = dir_ + "/MANIFEST";
            if (!std::filesystem::exists(path))
                return 0;
            std::ifstream ifs(path);
            uint64_t mi = 0, ms = 0;
            std::string line;
            while (std::getline(ifs, line))
            {
                std::istringstream iss(line);
                DBSSTMeta m;
                iss >> m.level >> m.file_id >> m.min_key >> m.max_key >> m.max_seq >> m.file_size >> m.filepath;
                if (iss.fail())
                    continue;
                if (std::filesystem::exists(m.filepath))
                {
                    m.reader = std::make_shared<SSTReader<uint64_t>>(m.filepath, m.file_id, cache_ns_);
                    (m.level == 0 ? l0_ssts_ : l1_ssts_).push_back(std::move(m));
                    mi = std::max(mi, m.file_id);
                    ms = std::max(ms, m.max_seq);
                }
            }
            next_file_id_.store(mi + 1, std::memory_order_release);
            return ms;
        }

        void SaveManifest()
        {
            std::shared_lock<std::shared_mutex> rl(sst_mtx_);
            SaveManifestInternal();
        }

        void SaveManifestInternal()
        {
            std::string tp = dir_ + "/MANIFEST.tmp", fp = dir_ + "/MANIFEST";
            {
                std::ofstream ofs(tp, std::ios::trunc);
                if (!ofs)
                    throw std::runtime_error("MANIFEST.tmp");
                auto w = [&](const DBSSTMeta &m)
                { ofs << m.level << " " << m.file_id << " " << m.min_key << " " << m.max_key << " " << m.max_seq << " " << m.file_size << " " << m.filepath << "\n"; };
                for (auto &s : l0_ssts_)
                    w(s);
                for (auto &s : l1_ssts_)
                    w(s);
                ofs.flush();
                int fd = ::open(tp.c_str(), O_RDONLY);
                if (fd >= 0)
                {
                    ::fdatasync(fd);
                    ::close(fd);
                }
            }
            std::filesystem::rename(tp, fp);
            int dfd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
            if (dfd >= 0)
            {
                ::fsync(dfd);
                ::close(dfd);
            }
        }
    };

} // namespace kvstore