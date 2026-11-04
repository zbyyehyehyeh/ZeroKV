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
    }

    ~WAL() {
        if (fd_ >= 0) { 
            FileUtils::SyncFile(fd_); 
            ::close(fd_); 
        }
    }

    void Append(uint64_t seq, uint64_t key, const char *val, uint32_t val_len, WALRecordType type) {
        uint32_t total = sizeof(WALRecordHeader) + val_len;
        
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

        std::lock_guard<std::mutex> lk(mu_);
        Status s = FileUtils::WriteFull(fd_, tl_buf.data(), total);
        if (!s.ok()) throw std::runtime_error("WAL Append failed: " + s.ToString());
    }

    void Sync() {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ >= 0) FileUtils::SyncFile(fd_);
    }

    void Rotate() {
        std::lock_guard<std::mutex> lk(mu_);
        if (fd_ >= 0) { 
            FileUtils::SyncFile(fd_); 
            ::close(fd_); 
        }
        fd_ = ::open(filepath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) throw std::runtime_error("WAL Rotate failed");
    }

private:
    std::string filepath_;
    int fd_ = -1;
    std::mutex mu_;
};