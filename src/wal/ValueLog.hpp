#pragma once
#include <array>
#include <string>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <filesystem>
#include <iostream>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <zlib.h>
#include <algorithm>
#include <random>
#include <functional>
#include <iomanip>
#include <stdexcept>
#include <climits>

#include "PinnableSlice.hpp"
#include "DataHandle.h"

namespace kvstore
{
    class ValueLog
    {
    public:
        struct VLogRecordHeader
        {
            uint32_t total_len;
            uint32_t crc32;
            uint32_t key_len;
            uint32_t val_len;
        };

        struct MappedFile
        {
            void *base = nullptr;
            size_t mapped_size = 0;
            std::atomic<size_t> valid_size{0};
            size_t file_size = 0;
            std::atomic<size_t> discardable_size{0};
            std::shared_ptr<void> mmap_holder;

            MappedFile() = default;

            MappedFile(MappedFile &&other) noexcept
                : base(other.base),
                  mapped_size(other.mapped_size),
                  valid_size(other.valid_size.load(std::memory_order_relaxed)),
                  file_size(other.file_size),
                  discardable_size(other.discardable_size.load()),
                  mmap_holder(std::move(other.mmap_holder))
            {
                other.base = nullptr;
                other.mapped_size = 0;
                other.file_size = 0;
                other.discardable_size = 0;
            }

            MappedFile &operator=(MappedFile &&other) noexcept
            {
                if (this != &other)
                {
                    base = other.base;
                    mapped_size = other.mapped_size;
                    valid_size.store(other.valid_size.load(std::memory_order_relaxed), std::memory_order_relaxed);
                    file_size = other.file_size;
                    discardable_size.store(other.discardable_size.load());
                    mmap_holder = std::move(other.mmap_holder);

                    other.base = nullptr;
                    other.mapped_size = 0;
                    other.file_size = 0;
                    other.discardable_size = 0;
                }
                return *this;
            }

            MappedFile(const MappedFile &) = delete;
            MappedFile &operator=(const MappedFile &) = delete;

            ~MappedFile() = default;
        };

        // 写段：每个段有独立的 fd / file_id / offset / 锁，写线程并行写不同段
        struct WriteSegment
        {
            int fd = -1;
            uint32_t file_id = 0;
            uint32_t offset = 0;
            std::mutex mtx;
        };

        explicit ValueLog(const std::string &dir) : dir_(dir)
        {
            std::filesystem::create_directories(dir);
            gc_fd_ = -1;
            gc_file_id_ = 0;
            gc_offset_ = 0;
            next_file_id_ = 1;
        }

        ~ValueLog()
        {
            for (auto &seg : segments_)
            {
                if (seg.fd >= 0)
                {
                    ::fdatasync(seg.fd);
                    ::close(seg.fd);
                    seg.fd = -1;
                }
            }
            if (gc_fd_ >= 0)
            {
                ::fdatasync(gc_fd_);
                ::close(gc_fd_);
                gc_fd_ = -1;
            }

            std::unique_lock<std::shared_mutex> wl(map_mtx_);
            for (auto &[id, mf] : mapped_files_)
                mf.mmap_holder.reset();
            mapped_files_.clear();
        }

        void RecoverAndTruncate()
        {
            std::unique_lock<std::shared_mutex> wl(map_mtx_);
            uint32_t max_fid = 0;

            for (auto &entry : std::filesystem::directory_iterator(dir_))
            {
                if (entry.is_regular_file() && entry.path().extension() == ".vlog")
                {
                    std::string fname = entry.path().filename().string();
                    try
                    {
                        auto pos = fname.find_first_of("0123456789");
                        auto end = fname.find(".vlog");
                        if (pos != std::string::npos && end != std::string::npos && end > pos)
                        {
                            uint32_t fid = std::stoul(fname.substr(pos, end - pos));
                            max_fid = std::max(max_fid, fid);
                        }
                    }
                    catch (...)
                    {
                        continue;
                    }
                }
            }

            next_file_id_ = max_fid + 1;

            for (uint32_t fid = 1; fid <= max_fid; ++fid)
            {
                std::string path = GetPath(fid);
                int fd = ::open(path.c_str(), O_RDWR);
                if (fd < 0)
                    continue;

                struct stat st;
                if (::fstat(fd, &st) != 0)
                {
                    ::close(fd);
                    continue;
                }
                size_t file_size = st.st_size;

                uint32_t valid_offset = 0;
                while (valid_offset + sizeof(VLogRecordHeader) <= file_size)
                {
                    VLogRecordHeader hdr;
                    if (::pread(fd, &hdr, sizeof(hdr), valid_offset) != sizeof(hdr))
                        break;
                    if (hdr.total_len < sizeof(hdr) || hdr.key_len != sizeof(uint64_t) || hdr.val_len > 16 * 1024 * 1024)
                        break;
                    if (valid_offset + hdr.total_len > file_size)
                        break;

                    std::vector<char> buf(hdr.key_len + hdr.val_len);
                    if (::pread(fd, buf.data(), buf.size(), valid_offset + sizeof(hdr)) != static_cast<ssize_t>(buf.size()))
                        break;

                    uint32_t crc = crc32(0L, Z_NULL, 0);
                    crc = crc32(crc, (const Bytef *)buf.data(), buf.size());
                    if (crc != hdr.crc32)
                        break;
                    valid_offset += hdr.total_len;
                }
                MappedFile mf;
                mf.mapped_size = std::max<size_t>(VLOG_ROLLOVER_THRESHOLD, valid_offset > 0 ? valid_offset * 2 : 4096);
                mf.valid_size.store(valid_offset);
                mf.file_size = valid_offset;

                if (valid_offset > 0)
                {
                    mf.base = ::mmap(nullptr, mf.mapped_size, PROT_READ, MAP_SHARED, fd, 0);
                    if (mf.base == MAP_FAILED)
                        mf.base = nullptr;
                }

                if (mf.base)
                {
                    mf.mmap_holder = std::shared_ptr<void>(mf.base, [sz = mf.mapped_size](void *p)
                                                           { if(p && p != MAP_FAILED) ::munmap(p, sz); });
                }
                mapped_files_[fid] = std::move(mf);
                ::close(fd);
            }

            // 开 N 个新写段（旧文件全部变只读，靠 GC 回收）
            wl.unlock();
            for (auto &seg : segments_)
            {
                OpenNewFileInternal(seg);
            }
        }

        void Append(uint64_t key, const char *value, uint32_t value_len, uint32_t &out_fid, uint32_t &out_off)
        {
            // 全局轮转选段，分散到不同段减少锁竞争
            uint32_t seg_idx = rr_.fetch_add(1, std::memory_order_relaxed) % segments_.size();
            WriteSegment &seg = segments_[seg_idx];

            std::unique_lock<std::mutex> lk(seg.mtx); // 每段一把锁

            uint32_t total_len = sizeof(VLogRecordHeader) + sizeof(uint64_t) + value_len;
            if (seg.offset > VLOG_ROLLOVER_THRESHOLD ||
                (seg.offset + total_len > VLOG_MAX_OFFSET))
            {
                OpenNewFileInternal(seg);
            }

            out_fid = seg.file_id;
            out_off = seg.offset;

            VLogRecordHeader hdr{total_len, 0, sizeof(uint64_t), value_len};
            uint32_t crc = crc32(0L, Z_NULL, 0);
            crc = crc32(crc, (const Bytef *)&key, sizeof(uint64_t));
            if (value_len > 0 && value)
                crc = crc32(crc, (const Bytef *)value, value_len);
            hdr.crc32 = crc;

            WriteRaw(seg, &hdr, sizeof(hdr));
            WriteRaw(seg, &key, sizeof(uint64_t));
            WriteRaw(seg, value, value_len);
        }

        bool Read(uint32_t file_id, uint32_t offset, uint32_t len, PinnableSlice &out_val)
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            auto it = mapped_files_.find(file_id);
            if (it == mapped_files_.end() || !it->second.base)
                return false;

            size_t vs = it->second.valid_size.load(std::memory_order_acquire);
            if (offset + sizeof(VLogRecordHeader) + sizeof(uint64_t) + len > vs)
                return false;

            const char *ptr = static_cast<const char *>(it->second.base) + offset + sizeof(VLogRecordHeader) + sizeof(uint64_t);
            out_val.pin_external(ptr, len, it->second.mmap_holder);
            return true;
        }

        // 恢复期校验句柄是否落在有效范围内（多段下逐文件检查 valid_size）
        bool IsHandleValid(uint32_t file_id, uint32_t offset, uint32_t len) const
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            auto it = mapped_files_.find(file_id);
            if (it == mapped_files_.end())
                return false;
            size_t vs = it->second.valid_size.load(std::memory_order_acquire);
            return offset + sizeof(VLogRecordHeader) + sizeof(uint64_t) + len <= vs;
        }

        void Sync()
        {
            for (auto &seg : segments_)
            {
                std::lock_guard<std::mutex> lk(seg.mtx);
                if (seg.fd >= 0)
                    ::fdatasync(seg.fd);
            }
        }

        uint32_t PickGCFile(double threshold = 0.5)
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            uint32_t best_fid = UINT32_MAX;
            double max_garbage_ratio = threshold;

            for (auto &[id, info] : mapped_files_)
            {
                size_t ds = info.discardable_size.load(std::memory_order_relaxed);

                if (IsActiveSegment(id) || (gc_file_id_ != 0 && id == gc_file_id_))
                    continue;
                if (!info.base || info.file_size == 0)
                    continue;

                double garbage_ratio = static_cast<double>(ds) / info.file_size;
                if (garbage_ratio >= max_garbage_ratio)
                {
                    max_garbage_ratio = garbage_ratio;
                    best_fid = id;
                }
            }

            return best_fid;
        }

        void RemoveFile(uint32_t file_id)
        {
            std::shared_ptr<void> holder_to_release;
            {
                std::unique_lock<std::shared_mutex> wl(map_mtx_);
                auto it = mapped_files_.find(file_id);
                if (it != mapped_files_.end())
                {
                    holder_to_release = std::move(it->second.mmap_holder);
                    mapped_files_.erase(it);
                }
            }
            std::string path = GetPath(file_id);
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }

        bool ReadRawForGC(uint32_t file_id, uint32_t offset, uint64_t &out_key, PinnableSlice &out_val, uint32_t &out_total_len)
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            auto it = mapped_files_.find(file_id);
            if (it == mapped_files_.end() || !it->second.base)
                return false;

            size_t vs = it->second.valid_size.load(std::memory_order_acquire);
            if (offset + sizeof(VLogRecordHeader) > vs)
                return false;

            const char *hdr_ptr = static_cast<const char *>(it->second.base) + offset;
            VLogRecordHeader hdr;
            std::memcpy(&hdr, hdr_ptr, sizeof(hdr));
            if (offset + hdr.total_len > vs)
                return false;

            if (hdr.total_len < sizeof(VLogRecordHeader) || hdr.key_len != sizeof(uint64_t))
                return false;

            std::memcpy(&out_key, hdr_ptr + sizeof(VLogRecordHeader), sizeof(uint64_t));
            const char *data_ptr = hdr_ptr + sizeof(VLogRecordHeader) + hdr.key_len;

            uint32_t crc = crc32(0L, Z_NULL, 0);
            crc = crc32(crc, (const Bytef *)(hdr_ptr + sizeof(VLogRecordHeader)), hdr.key_len + hdr.val_len);
            if (crc != hdr.crc32)
                return false;

            out_val.pin_external(data_ptr, hdr.val_len, it->second.mmap_holder);
            out_total_len = hdr.total_len;
            return true;
        }

        uint32_t PickGCFileForce()
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            uint32_t oldest_fid = UINT32_MAX;
            for (auto &[id, info] : mapped_files_)
            {
                if (IsActiveSegment(id) || (gc_file_id_ != 0 && id == gc_file_id_))
                    continue;
                if (!info.base || info.file_size == 0)
                    continue;
                if (id < oldest_fid)
                    oldest_fid = id;
            }
            return oldest_fid;
        }

        void RollToNewFile()
        {
            for (auto &seg : segments_)
            {
                std::lock_guard<std::mutex> lk(seg.mtx);
                OpenNewFileInternal(seg);
            }
        }

        DataHandle AppendForGC(uint64_t key, const char *value, uint32_t value_len)
        {
            std::lock_guard<std::mutex> lk(gc_mtx_);

            uint32_t total_len = sizeof(VLogRecordHeader) + sizeof(uint64_t) + value_len;

            if (gc_fd_ < 0 ||
                gc_offset_ > VLOG_ROLLOVER_THRESHOLD ||
                (gc_offset_ + total_len > VLOG_MAX_OFFSET))
            {
                OpenNewGCFileInternal();
            }

            VLogRecordHeader hdr{total_len, 0, sizeof(uint64_t), value_len};

            uint32_t crc = crc32(0L, Z_NULL, 0);
            crc = crc32(crc, (const Bytef *)&key, sizeof(uint64_t));
            if (value_len > 0 && value)
                crc = crc32(crc, (const Bytef *)value, value_len);
            hdr.crc32 = crc;

            thread_local std::vector<char> buf;
            if (buf.size() < total_len)
                buf.resize(total_len * 2);
            std::memcpy(buf.data(), &hdr, sizeof(hdr));
            std::memcpy(buf.data() + sizeof(hdr), &key, sizeof(uint64_t));
            if (value_len > 0)
                std::memcpy(buf.data() + sizeof(hdr) + sizeof(uint64_t), value, value_len);

            ssize_t written = ::write(gc_fd_, buf.data(), total_len);
            if (written < 0)
                throw std::runtime_error("GC VLog write failed");

            uint32_t offset = gc_offset_;
            gc_offset_ += total_len;

            {
                std::unique_lock<std::shared_mutex> wl(map_mtx_);
                auto it = mapped_files_.find(gc_file_id_);
                if (it != mapped_files_.end())
                {
                    auto &mf = it->second;
                    mf.valid_size.store(gc_offset_, std::memory_order_release);
                    mf.file_size = gc_offset_;

                    if (gc_offset_ > mf.mapped_size)
                    {
                        size_t new_size = std::max<size_t>(VLOG_ROLLOVER_THRESHOLD, gc_offset_ * 2);
                        void *new_base = ::mmap(nullptr, new_size, PROT_READ, MAP_SHARED, gc_fd_, 0);
                        if (new_base == MAP_FAILED)
                            new_base = nullptr;

                        auto old_holder = std::move(mf.mmap_holder);

                        mf.base = new_base;
                        mf.mapped_size = new_size;
                        mf.mmap_holder = std::shared_ptr<void>(new_base, [sz = new_size](void *p)
                                                               { if(p && p != MAP_FAILED) ::munmap(p, sz); });

                        wl.unlock();
                        old_holder.reset();
                    }
                }
            }

            return DataHandle::make_vlog(DataType::RAW, gc_file_id_, offset, value_len);
        }

        void AddDiscardableSize(uint32_t file_id, uint32_t size)
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            auto it = mapped_files_.find(file_id);
            if (it != mapped_files_.end())
            {
                it->second.discardable_size += size;
            }
        }

    private:
        static constexpr uint32_t VLOG_MAX_OFFSET = (1U << 30) - 1;
        static constexpr uint32_t VLOG_ROLLOVER_THRESHOLD = 900 * 1024 * 1024;
        static constexpr uint32_t kNumSegments = 8; // 段数 ≥ 写线程数

        std::string GetPath(uint32_t id) { return dir_ + "/vlog_" + std::to_string(id) + ".vlog"; }

        uint32_t AllocateFileId()
        {
            uint32_t id = next_file_id_++;
            if (id > DataHandle::MASK_FILEID)
            {
                throw std::runtime_error("VLog FileID exceeds limit. Need to expand DataHandle bit layout.");
            }
            return id;
        }

        bool IsActiveSegment(uint32_t id) const
        {
            for (auto &seg : segments_)
                if (seg.file_id == id)
                    return true;
            return false;
        }

        void OpenNewFileInternal(WriteSegment &seg)
        {
            int old_fd = seg.fd;
            seg.fd = -1;
            if (old_fd >= 0)
            {
                ::fdatasync(old_fd);
                ::close(old_fd);
            }

            seg.file_id = AllocateFileId();
            seg.offset = 0;
            std::string path = GetPath(seg.file_id);
            int new_fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (new_fd < 0)
                throw std::runtime_error("Failed to open VLog file");

            MappedFile mf;
            mf.valid_size.store(0);
            mf.file_size = 0;

            mf.mapped_size = VLOG_ROLLOVER_THRESHOLD;
            mf.base = ::mmap(nullptr, mf.mapped_size, PROT_READ, MAP_SHARED, new_fd, 0);
            if (mf.base == MAP_FAILED)
                mf.base = nullptr;
            if (mf.base)
            {
                mf.mmap_holder = std::shared_ptr<void>(mf.base, [sz = mf.mapped_size](void *p)
                                                       { if(p && p != MAP_FAILED) ::munmap(p, sz); });
            }

            {
                std::unique_lock<std::shared_mutex> wl(map_mtx_);
                mapped_files_[seg.file_id] = std::move(mf);
            }
            seg.fd = new_fd;
        }

        void OpenNewGCFileInternal()
        {
            int old_fd = gc_fd_;
            gc_fd_ = -1;
            if (old_fd >= 0)
            {
                ::fdatasync(old_fd);
                ::close(old_fd);
            }

            gc_file_id_ = AllocateFileId();
            gc_offset_ = 0;
            std::string gc_path = GetPath(gc_file_id_);
            int new_fd = ::open(gc_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (new_fd < 0)
                throw std::runtime_error("Open GC VLog failed");

            MappedFile mf;
            mf.mapped_size = VLOG_ROLLOVER_THRESHOLD;
            mf.valid_size.store(0);
            mf.file_size = 0;
            mf.base = ::mmap(nullptr, mf.mapped_size, PROT_READ, MAP_SHARED, new_fd, 0);
            if (mf.base == MAP_FAILED)
                mf.base = nullptr;
            if (mf.base)
            {
                mf.mmap_holder = std::shared_ptr<void>(mf.base, [sz = mf.mapped_size](void *p)
                                                       { if(p && p != MAP_FAILED) ::munmap(p, sz); });
            }

            {
                std::unique_lock<std::shared_mutex> wl(map_mtx_);
                mapped_files_[gc_file_id_] = std::move(mf);
            }
            gc_fd_ = new_fd;
        }

        void WriteRaw(WriteSegment &seg, const void *data, size_t len)
        {
            const char *ptr = static_cast<const char *>(data);
            size_t remaining = len;
            while (remaining > 0)
            {
                ssize_t w = ::write(seg.fd, ptr, remaining);
                if (w < 0)
                {
                    if (errno == EINTR)
                        continue;
                    throw std::runtime_error("VLog write failed");
                }
                ptr += w;
                remaining -= w;
                seg.offset += w;
            }

            std::unique_lock<std::shared_mutex> wl(map_mtx_);
            auto it = mapped_files_.find(seg.file_id);
            if (it != mapped_files_.end())
            {
                auto &mf = it->second;
                mf.valid_size.store(seg.offset, std::memory_order_release);
                mf.file_size = seg.offset;

                if (seg.offset > mf.mapped_size)
                {
                    size_t new_size = std::max<size_t>(VLOG_ROLLOVER_THRESHOLD, seg.offset * 2);
                    void *new_base = ::mmap(nullptr, new_size, PROT_READ, MAP_SHARED, seg.fd, 0);
                    if (new_base == MAP_FAILED)
                        new_base = nullptr;

                    auto old_holder = std::move(mf.mmap_holder);

                    mf.base = new_base;
                    mf.mapped_size = new_size;
                    mf.mmap_holder = std::shared_ptr<void>(new_base, [sz = new_size](void *p)
                                                           { if(p && p != MAP_FAILED) ::munmap(p, sz); });

                    wl.unlock();
                    old_holder.reset();
                }
            }
        }

        std::string dir_;

        std::array<WriteSegment, kNumSegments> segments_; // N 个写段（array：mutex 不可移动）
        std::atomic<uint32_t> rr_{0};                     // 全局轮转计数器

        int gc_fd_ = -1;
        uint32_t gc_file_id_ = 0;
        uint32_t gc_offset_ = 0;
        std::mutex gc_mtx_;

        std::atomic<uint32_t> next_file_id_{1};

        std::unordered_map<uint32_t, MappedFile> mapped_files_;
        mutable std::shared_mutex map_mtx_;
    };

} // namespace kvstore