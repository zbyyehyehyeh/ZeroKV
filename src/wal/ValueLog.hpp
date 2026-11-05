#pragma once
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

        explicit ValueLog(const std::string &dir) : dir_(dir)
        {
            std::filesystem::create_directories(dir);
            current_file_id_ = 0;
            current_offset_ = 0;
            current_fd_ = -1;
            gc_fd_ = -1;
            gc_file_id_ = 0;
            next_file_id_ = 1;
        }

        ~ValueLog()
        {
            if (current_fd_ >= 0)
            {
                ::fdatasync(current_fd_);
                ::close(current_fd_);
                current_fd_ = -1;
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

            if (max_fid == 0)
            {
                wl.unlock();
                OpenNewFileInternal();
                return;
            }

            uint32_t active_fid = 0;
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

                if (valid_offset > 0)
                    active_fid = fid;
                ::close(fd);
            }

            wl.unlock();

            if (active_fid > 0)
            {
                std::unique_lock<std::shared_mutex> wl2(map_mtx_);
                current_file_id_ = active_fid;
                current_offset_ = mapped_files_[active_fid].valid_size.load();

                std::string path = GetPath(current_file_id_);
                current_fd_ = ::open(path.c_str(), O_RDWR | O_APPEND);

                auto &mf = mapped_files_[active_fid];
                if (!mf.base && current_offset_ > 0)
                {
                    mf.base = ::mmap(nullptr, mf.mapped_size, PROT_READ, MAP_SHARED, current_fd_, 0);
                    if (mf.base == MAP_FAILED)
                        mf.base = nullptr;
                    if (mf.base)
                    {
                        mf.mmap_holder = std::shared_ptr<void>(mf.base, [sz = mf.mapped_size](void *p)
                                                               { if(p && p != MAP_FAILED) ::munmap(p, sz); });
                    }
                }
                wl2.unlock();

                if (current_offset_ > VLOG_ROLLOVER_THRESHOLD)
                    OpenNewFileInternal();
            }
            else
            {
                OpenNewFileInternal();
            }
        }

        void Append(uint64_t key, const char *value, uint32_t value_len, uint32_t &out_fid, uint32_t &out_off)
        {
            std::unique_lock<std::mutex> lk(write_mtx_);

            uint32_t total_len = sizeof(VLogRecordHeader) + sizeof(uint64_t) + value_len;
            if (current_offset_ > VLOG_ROLLOVER_THRESHOLD ||
                (current_offset_ + total_len > VLOG_MAX_OFFSET))
            {
                OpenNewFileInternal();
            }

            out_fid = current_file_id_;
            out_off = current_offset_;

            VLogRecordHeader hdr{total_len, 0, sizeof(uint64_t), value_len};

            uint32_t crc = crc32(0L, Z_NULL, 0);
            crc = crc32(crc, (const Bytef *)&key, sizeof(uint64_t));
            if (value_len > 0 && value)
                crc = crc32(crc, (const Bytef *)value, value_len);
            hdr.crc32 = crc;

            WriteRaw(&hdr, sizeof(hdr));
            WriteRaw(&key, sizeof(uint64_t));
            WriteRaw(value, value_len);
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

        uint32_t GetCurrentFileId() const { return current_file_id_; }
        uint32_t GetCurrentValidSize() const { return current_offset_; }

        void Sync()
        {
            std::unique_lock<std::mutex> lk(write_mtx_);
            if (current_fd_ >= 0)
                ::fdatasync(current_fd_);
        }

        uint32_t PickGCFile(double threshold = 0.5)
        {
            std::shared_lock<std::shared_mutex> rl(map_mtx_);
            uint32_t best_fid = UINT32_MAX;
            double max_garbage_ratio = threshold;

            for (auto &[id, info] : mapped_files_)
            {
                size_t ds = info.discardable_size.load(std::memory_order_relaxed);
                
                if (id == current_file_id_ || (gc_file_id_ != 0 && id == gc_file_id_))
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
                if (id == current_file_id_ || (gc_file_id_ != 0 && id == gc_file_id_))
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
            std::unique_lock<std::mutex> lk(write_mtx_);
            OpenNewFileInternal();
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
            if (it != mapped_files_.end()) {
                it->second.discardable_size += size;
            }
        }

    private:
        
        static constexpr uint32_t VLOG_MAX_OFFSET = (1U << 30) - 1;
        static constexpr uint32_t VLOG_ROLLOVER_THRESHOLD = 900 * 1024 * 1024;

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

        void OpenNewFileInternal()
        {
            int old_fd = current_fd_;
            current_fd_ = -1;
            if (old_fd >= 0)
            {
                ::fdatasync(old_fd);
                ::close(old_fd);
            }

            current_file_id_ = AllocateFileId();
            current_offset_ = 0;
            std::string path = GetPath(current_file_id_);
            int new_fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
            if (new_fd < 0)
                throw std::runtime_error("Failed to open VLog file");

            MappedFile mf;
            mf.valid_size.store(0);
            mf.file_size = 0;  // ✅ FIX: 显式初始化 file_size

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
                mapped_files_[current_file_id_] = std::move(mf);
            }
            current_fd_ = new_fd;
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
            mf.file_size = 0;  // ✅ FIX: 显式初始化 file_size
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

        void WriteRaw(const void *data, size_t len)
        {
            const char *ptr = static_cast<const char *>(data);
            size_t remaining = len;
            while (remaining > 0)
            {
                ssize_t w = ::write(current_fd_, ptr, remaining);
                if (w < 0)
                {
                    if (errno == EINTR)
                        continue;
                    throw std::runtime_error("VLog write failed");
                }
                ptr += w;
                remaining -= w;
                current_offset_ += w;
            }

            std::unique_lock<std::shared_mutex> wl(map_mtx_);
            auto it = mapped_files_.find(current_file_id_);
            if (it != mapped_files_.end())
            {
                auto &mf = it->second;
                mf.valid_size.store(current_offset_, std::memory_order_release);
                mf.file_size = current_offset_;

                if (current_offset_ > mf.mapped_size)
                {
                    size_t new_size = std::max<size_t>(VLOG_ROLLOVER_THRESHOLD, current_offset_ * 2);
                    void *new_base = ::mmap(nullptr, new_size, PROT_READ, MAP_SHARED, current_fd_, 0);
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
        int current_fd_ = -1;
        uint32_t current_file_id_ = 0;
        uint32_t current_offset_ = 0;
        std::mutex write_mtx_;

        int gc_fd_ = -1;
        uint32_t gc_file_id_ = 0;
        uint32_t gc_offset_ = 0;
        std::mutex gc_mtx_;

        std::atomic<uint32_t> next_file_id_{1};

        std::unordered_map<uint32_t, MappedFile> mapped_files_;
        mutable std::shared_mutex map_mtx_;
    };

} // namespace kvstore