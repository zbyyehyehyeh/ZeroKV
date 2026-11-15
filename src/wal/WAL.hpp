#pragma once
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <unistd.h>
#include <vector>
#include <zlib.h>
#include <functional>
#include <stdexcept>
#include <cerrno>

#include "Status.h"
#include "FileUtils.hpp"

enum class WALRecordType : uint8_t { PUT = 1, DELETE = 2 };

#pragma pack(push, 1)
struct WALRecordHeader {
    uint32_t total_len;
    uint32_t crc32;
    uint64_t seq;
    uint64_t key;
    uint32_t val_len;
    uint8_t type;
};
#pragma pack(pop)

class WAL {
public:
    static uint64_t Recover(const std::string &filepath,
                            const std::function<void(uint64_t, uint64_t, const char*, uint32_t, WALRecordType)> &cb) {
        int rfd = ::open(filepath.c_str(), O_RDONLY);
        if (rfd < 0) {
            if (errno == ENOENT) return 0;
            throw std::runtime_error("WAL open failed: " + filepath);
        }

        constexpr size_t BUF_SIZE = 4 * 1024 * 1024;
        std::vector<char> buf(BUF_SIZE + sizeof(WALRecordHeader) + 1024 * 1024);
        size_t residual = 0;
        ssize_t n;
        uint64_t max_seq = 0;

        while ((n = ::read(rfd, buf.data() + residual, BUF_SIZE)) > 0) {
            size_t valid_len = residual + static_cast<size_t>(n);
            size_t off = 0;

            while (off < valid_len) {
                if (off + sizeof(WALRecordHeader) > valid_len) break;

                WALRecordHeader hdr;
                std::memcpy(&hdr, buf.data() + off, sizeof(hdr));

                if (hdr.total_len < sizeof(WALRecordHeader) || off + hdr.total_len > valid_len) break;

                uint32_t expected_crc = crc32(0L, Z_NULL, 0);
                expected_crc = crc32(expected_crc, (const Bytef*)&hdr.seq, sizeof(hdr.seq));
                expected_crc = crc32(expected_crc, (const Bytef*)&hdr.key, sizeof(hdr.key));
                expected_crc = crc32(expected_crc, (const Bytef*)&hdr.val_len, sizeof(hdr.val_len));
                expected_crc = crc32(expected_crc, (const Bytef*)&hdr.type, sizeof(hdr.type));
                if (hdr.val_len > 0) {
                    expected_crc = crc32(expected_crc, (const Bytef*)(buf.data() + off + sizeof(hdr)), hdr.val_len);
                }

                if (hdr.crc32 != expected_crc) {
                    off = valid_len;
                    break;
                }

                cb(hdr.seq, hdr.key, buf.data() + off + sizeof(hdr), hdr.val_len, static_cast<WALRecordType>(hdr.type));
                max_seq = std::max(max_seq, hdr.seq);
                off += hdr.total_len;
            }

            residual = valid_len - off;
            if (residual > 0 && off > 0) {
                std::memmove(buf.data(), buf.data() + off, residual);
            }
        }

        ::close(rfd);
        return max_seq;
    }

    explicit WAL(const std::string &filepath) : filepath_(filepath) {
        fd_ = ::open(filepath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) throw std::runtime_error("WAL open failed: " + filepath);
        group_buffer_.resize(kGroupBufferSize);
    }

    ~WAL() {
        if (fd_ >= 0) {
            FlushGroupLocked();          // 关闭前把缓冲区刷出去，否则丢记录
            FileUtils::SyncFile(fd_);
            ::close(fd_);
        }
    }

    // Group commit 版 Append：
    //   每条记录不再单独 write()，而是 append 到共享 4MB 缓冲区，满了才批量 write()。
    //   这同时减少 write() 系统调用次数、ext4 元数据开销，以及 mu_ 的锁持有时间。
    void Append(uint64_t seq, uint64_t key, const char *val, uint32_t val_len, WALRecordType type) {
        uint32_t total = sizeof(WALRecordHeader) + val_len;

        // 记录组装在 TLS 缓冲里做（无锁、可并行）
        thread_local std::vector<char> tl_buf;
        if (tl_buf.size() < total) tl_buf.resize(total * 2);

        WALRecordHeader hdr{total, 0, seq, key, val_len, static_cast<uint8_t>(type)};
        std::memcpy(tl_buf.data(), &hdr, sizeof(hdr));
        if (val_len > 0 && val) {
            std::memcpy(tl_buf.data() + sizeof(hdr), val, val_len);
        }

        uint32_t crc = crc32(0L, Z_NULL, 0);
        crc = crc32(crc, (const Bytef*)&hdr.seq, sizeof(hdr.seq));
        crc = crc32(crc, (const Bytef*)&hdr.key, sizeof(hdr.key));
        crc = crc32(crc, (const Bytef*)&hdr.val_len, sizeof(hdr.val_len));
        crc = crc32(crc, (const Bytef*)&hdr.type, sizeof(hdr.type));
        if (val_len > 0 && val) crc = crc32(crc, (const Bytef*)val, val_len);

        std::memcpy(tl_buf.data() + 4, &crc, 4);

        // 临界区里只有 memcpy（纳秒级），不再是 write()（微秒级）→ 锁竞争也大幅下降
        std::lock_guard<std::mutex> lk(mu_);
        if (group_offset_ + total > group_buffer_.size()) {
            FlushGroupLocked();
        }
        std::memcpy(group_buffer_.data() + group_offset_, tl_buf.data(), total);
        group_offset_ += total;
    }

    void Sync() {
        std::lock_guard<std::mutex> lk(mu_);
        FlushGroupLocked();          // 先刷缓冲区，否则 fsync 覆盖不到缓冲区里的记录
        if (fd_ >= 0) FileUtils::SyncFile(fd_);
    }

    void Rotate() {
        std::lock_guard<std::mutex> lk(mu_);
        FlushGroupLocked();          // 先刷缓冲区，否则 truncate 会丢掉缓冲区的记录
        if (fd_ >= 0) {
            FileUtils::SyncFile(fd_);
            ::close(fd_);
        }
        fd_ = ::open(filepath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) throw std::runtime_error("WAL Rotate failed");
        group_offset_ = 0;
    }

    // 显式刷缓冲区到 page cache（不 fsync）。供外部在需要保证"缓冲区已落到文件"时调用。
    void FlushGroup() {
        std::lock_guard<std::mutex> lk(mu_);
        FlushGroupLocked();
    }

private:
    static constexpr size_t kGroupBufferSize = 4 * 1024 * 1024;  // 4MB

    std::string filepath_;
    int fd_ = -1;
    std::mutex mu_;
    std::vector<char> group_buffer_;   // 共享写缓冲区
    size_t group_offset_ = 0;          // 当前缓冲区的写位置

    // 前置条件：调用方已持有 mu_
    void FlushGroupLocked() {
        if (group_offset_ == 0) return;
        Status s = FileUtils::WriteFull(fd_, group_buffer_.data(), group_offset_);
        if (!s.ok()) throw std::runtime_error("WAL FlushGroup failed: " + s.ToString());
        group_offset_ = 0;
    }
};