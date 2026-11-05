#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <unordered_map>
#include <list>
#include <mutex>
#include <array>
#include <memory>
#include <lz4.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>
#include <cerrno>
#include <iostream>

#include "PinnableSlice.hpp"
#include "BloomFilter.hpp"     // ✅ 已升级为 Prefix 版本
#include "KVStoreIterator.hpp" // ✅ 改动②：SST 迭代器实现统一迭代器接口

template <typename KeyType>
class SSTReader;

// ==================== SSTWriter ====================
template <typename KeyType>
class SSTWriter
{
public:
    static constexpr int RESTART_INTERVAL = 16;
    static constexpr uint8_t BLOCK_TYPE_LZ4 = 1;
    static constexpr uint8_t BLOCK_TYPE_RAW = 0;

    explicit SSTWriter(std::string filepath, size_t block_size = 65536)
        : filepath_(std::move(filepath)), block_size_(block_size)
    {
        fd_ = ::open(filepath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0)
            throw std::runtime_error("Failed to open SST file for writing: " + filepath_);
        block_buffer_.reserve(block_size * 2);
        compress_buffer_.resize(LZ4_COMPRESSBOUND(block_size * 2));
    }

    ~SSTWriter()
    {
        if (fd_ >= 0)
        {
            try
            {
                Finish();
            }
            catch (...)
            {
            }
            if (fd_ >= 0)
            {
                ::close(fd_);
                fd_ = -1;
            }
        }
    }

    SSTWriter(const SSTWriter &) = delete;
    SSTWriter &operator=(const SSTWriter &) = delete;

    void Add(const KeyType &key, const DataHandle &value, uint64_t seq = 0)
    {
        if (num_entries_ == 0)
            min_key_ = key;
        max_key_ = key;
        num_entries_++;
        // ✅ 收集完整 key，BloomFilter 内部会自动提取前缀并去重
        keys_for_filter_.push_back(static_cast<uint64_t>(key));
        block_seqs_.push_back(seq);

        if (current_block_entries_ % RESTART_INTERVAL == 0)
            restart_points_.push_back(static_cast<uint32_t>(block_buffer_.size()));
        if (current_block_entries_ == 0)
            index_.emplace_back(key, file_offset_);

        uint32_t k_len = sizeof(KeyType);
        uint32_t v_len = sizeof(uint64_t);

        AppendRaw(&k_len, 4);
        AppendRaw(&key, sizeof(KeyType));
        AppendRaw(&v_len, 4);
        AppendRaw(&value.raw, sizeof(uint64_t));

        current_block_entries_++;
        if (block_buffer_.size() >= block_size_)
            FlushDataBlock();
    }

    void Finish()
    {
        if (fd_ < 0)
            return;
        try
        {
            if (current_block_entries_ > 0)
                FlushDataBlock();
            WriteMetaBlock();
            WriteFilterBlock();
            WriteFooter();
            if (::fdatasync(fd_) < 0)
                throw std::runtime_error("fdatasync failed");
        }
        catch (...)
        {
            ::close(fd_);
            fd_ = -1;
            throw;
        }
        if (::close(fd_) < 0)
        {
            fd_ = -1;
            throw std::runtime_error("close failed");
        }
        fd_ = -1;
    }

    KeyType GetMinKey() const { return min_key_; }
    KeyType GetMaxKey() const { return max_key_; }
    uint64_t GetFileSize() const { return file_offset_; }
    uint64_t GetEntryCount() const { return num_entries_; }

private:
    inline void AppendRaw(const void *data, size_t len)
    {
        const uint8_t *p = static_cast<const uint8_t *>(data);
        block_buffer_.insert(block_buffer_.end(), p, p + len);
    }

    void WriteToFile(const void *data, size_t len)
    {
        const char *ptr = static_cast<const char *>(data);
        while (len > 0)
        {
            ssize_t written = ::write(fd_, ptr, len);
            if (written < 0)
            {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("write to SST failed");
            }
            ptr += written;
            len -= written;
        }
    }

    void FlushDataBlock()
    {
        for (uint64_t s : block_seqs_)
            AppendRaw(&s, 8);
        uint32_t num_seqs = static_cast<uint32_t>(block_seqs_.size());
        AppendRaw(&num_seqs, 4);
        for (uint32_t rp : restart_points_)
            AppendRaw(&rp, 4);
        uint32_t num_restarts = static_cast<uint32_t>(restart_points_.size());
        AppendRaw(&num_restarts, 4);

        uint32_t orig_size = static_cast<uint32_t>(block_buffer_.size());
        int compressed_size = LZ4_compress_default(
            reinterpret_cast<const char *>(block_buffer_.data()),
            compress_buffer_.data(), orig_size, static_cast<int>(compress_buffer_.size()));

        uint32_t comp_size = (compressed_size > 0 && static_cast<uint32_t>(compressed_size) < orig_size)
                                 ? static_cast<uint32_t>(compressed_size)
                                 : orig_size;
        uint8_t type = (comp_size < orig_size) ? BLOCK_TYPE_LZ4 : BLOCK_TYPE_RAW;

        WriteToFile(&orig_size, 4);
        WriteToFile(&comp_size, 4);
        if (type == BLOCK_TYPE_LZ4)
            WriteToFile(compress_buffer_.data(), comp_size);
        else
            WriteToFile(block_buffer_.data(), orig_size);

        uint32_t crc = crc32(0L, Z_NULL, 0);
        crc = crc32(crc, reinterpret_cast<const Bytef *>(&type), 1);
        if (type == BLOCK_TYPE_LZ4)
            crc = crc32(crc, reinterpret_cast<const Bytef *>(compress_buffer_.data()), comp_size);
        else
            crc = crc32(crc, reinterpret_cast<const Bytef *>(block_buffer_.data()), orig_size);

        WriteToFile(&type, 1);
        WriteToFile(&crc, 4);

        file_offset_ += 4 + 4 + comp_size + 1 + 4;
        block_buffer_.clear();
        block_seqs_.clear();
        current_block_entries_ = 0;
        restart_points_.clear();
    }

    void WriteMetaBlock()
    {
        meta_offset_ = file_offset_;
        uint32_t index_size = static_cast<uint32_t>(index_.size());
        WriteToFile(&index_size, 4);
        file_offset_ += 4;
        for (const auto &[key, offset] : index_)
        {
            uint32_t k_len = sizeof(KeyType);
            WriteToFile(&k_len, 4);
            WriteToFile(&offset, 8);
            WriteToFile(&key, sizeof(KeyType));
            file_offset_ += 4 + 8 + sizeof(KeyType);
        }
    }

    void WriteFilterBlock()
    {
        filter_offset_ = file_offset_;
        // ✅ BloomFilter::BuildFilter 内部已处理前缀提取与去重
        std::vector<uint8_t> filter_data = BloomFilter::BuildFilter(keys_for_filter_);
        filter_size_ = filter_data.size();
        if (filter_size_ > 0)
        {
            WriteToFile(filter_data.data(), filter_size_);
            file_offset_ += filter_size_;
        }
        // ✅ 写入完成后释放内存，避免 Finish 后仍持有大量 key
        keys_for_filter_.clear();
        keys_for_filter_.shrink_to_fit();
    }

    void WriteFooter()
    {
        uint64_t magic = 0x524F434B53444221ULL;
        WriteToFile(&meta_offset_, 8);
        WriteToFile(&filter_offset_, 8);
        WriteToFile(&filter_size_, 8);
        WriteToFile(&magic, 8);
        file_offset_ += 32;
    }

    std::string filepath_;
    int fd_ = -1;
    size_t block_size_;
    std::vector<uint8_t> block_buffer_;
    std::vector<char> compress_buffer_;
    std::vector<uint32_t> restart_points_;
    std::vector<uint64_t> block_seqs_;
    uint32_t current_block_entries_ = 0;
    uint64_t file_offset_ = 0;
    std::vector<std::pair<KeyType, uint64_t>> index_;
    uint64_t meta_offset_ = 0, filter_offset_ = 0, filter_size_ = 0;
    KeyType min_key_{}, max_key_{};
    uint64_t num_entries_ = 0;
    std::vector<uint64_t> keys_for_filter_;
};

// ==================== BlockCache（✅ 加实例命名空间） ====================
// 修复：缓存 key 原为 (file_id, block_offset)，而每个 DB 实例的 file_id 都从 1
// 开始，多个实例（或不同目录）的块会互相污染。现在 key 增加 cache_ns 维度，
// 每个 DB 实例分配唯一命名空间。
class BlockCache
{
    static constexpr size_t NUM_SHARDS = 16;
    struct CacheKey
    {
        uint64_t cache_ns;
        uint64_t file_id, block_offset;
        bool operator==(const CacheKey &o) const { return cache_ns == o.cache_ns && file_id == o.file_id && block_offset == o.block_offset; }
    };
    struct CacheKeyHash
    {
        size_t operator()(const CacheKey &k) const
        {
            size_t h = std::hash<uint64_t>{}(k.cache_ns);
            h ^= std::hash<uint64_t>{}(k.file_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<uint64_t>{}(k.block_offset) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };
    struct Shard
    {
        std::mutex mu;
        size_t capacity;
        std::list<std::pair<CacheKey, std::shared_ptr<std::vector<uint8_t>>>> list;
        std::unordered_map<CacheKey, typename decltype(list)::iterator, CacheKeyHash> map;
        explicit Shard(size_t cap) : capacity(cap) {}
    };
    std::array<std::unique_ptr<Shard>, NUM_SHARDS> shards_;
    static size_t shard_index(uint64_t cache_ns, uint64_t file_id, uint64_t offset) { return ((cache_ns * 2654435761ULL) ^ (file_id * 2654435761ULL) ^ offset) % NUM_SHARDS; }

public:
    static BlockCache &instance(size_t total_blocks = 8192)
    {
        static BlockCache inst(total_blocks);
        return inst;
    }
    explicit BlockCache(size_t total_blocks)
    {
        size_t per_shard = std::max<size_t>(1, total_blocks / NUM_SHARDS);
        for (size_t i = 0; i < NUM_SHARDS; ++i)
            shards_[i] = std::make_unique<Shard>(per_shard);
    }
    std::shared_ptr<std::vector<uint8_t>> get(uint64_t cache_ns, uint64_t file_id, uint64_t block_offset)
    {
        auto &shard = *shards_[shard_index(cache_ns, file_id, block_offset)];
        CacheKey ck{cache_ns, file_id, block_offset};
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.map.find(ck);
        if (it == shard.map.end())
            return nullptr;
        shard.list.splice(shard.list.begin(), shard.list, it->second);
        return it->second->second;
    }
    void put(uint64_t cache_ns, uint64_t file_id, uint64_t block_offset, std::shared_ptr<std::vector<uint8_t>> data)
    {
        auto &shard = *shards_[shard_index(cache_ns, file_id, block_offset)];
        CacheKey ck{cache_ns, file_id, block_offset};
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.map.find(ck);
        if (it != shard.map.end())
        {
            shard.list.splice(shard.list.begin(), shard.list, it->second);
            return;
        }
        if (shard.list.size() >= shard.capacity)
        {
            shard.map.erase(shard.list.back().first);
            shard.list.pop_back();
        }
        shard.list.emplace_front(ck, data);
        shard.map[ck] = shard.list.begin();
    }
    void clear()
    {
        for (size_t i = 0; i < NUM_SHARDS; ++i)
        {
            std::lock_guard<std::mutex> lock(shards_[i]->mu);
            shards_[i]->map.clear();
            shards_[i]->list.clear();
        }
    }
};

// ==================== SSTReader ====================
template <typename KeyType>
class SSTReader
{
public:
    class Iterator;
    static constexpr uint8_t BLOCK_TYPE_LZ4 = 1;
    static constexpr uint8_t BLOCK_TYPE_RAW = 0;
    static constexpr uint32_t ENTRY_SIZE = 4 + sizeof(KeyType) + 4 + sizeof(uint64_t);

    explicit SSTReader(const std::string &filepath, uint64_t file_id, uint64_t cache_ns = 0)
        : filepath_(filepath), file_id_(file_id), cache_ns_(cache_ns)
    {
        int fd = ::open(filepath.c_str(), O_RDONLY);
        if (fd < 0)
            throw std::runtime_error("SSTReader open failed: " + filepath);
        struct stat st;
        if (::fstat(fd, &st) < 0)
        {
            ::close(fd);
            throw std::runtime_error("fstat failed");
        }
        file_size_ = st.st_size;
        if (file_size_ < 32)
        {
            ::close(fd);
            throw std::runtime_error("SST file too small");
        }

        void *base = ::mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd, 0);
        if (base == MAP_FAILED)
        {
            ::close(fd);
            throw std::runtime_error("mmap failed");
        }
        ::madvise(base, file_size_, MADV_RANDOM);
        ::close(fd);

        mmap_holder_ = std::shared_ptr<void>(base, [size = file_size_](void *p)
                                             { if (p && p != MAP_FAILED) ::munmap(p, size); });
        const uint8_t *data = static_cast<const uint8_t *>(base);
        std::memcpy(&meta_offset_, data + file_size_ - 32, 8);
        uint64_t f_offset = 0, f_size = 0, magic = 0;
        std::memcpy(&f_offset, data + file_size_ - 24, 8);
        std::memcpy(&f_size, data + file_size_ - 16, 8);
        std::memcpy(&magic, data + file_size_ - 8, 8);
        if (magic != 0x524F434B53444221ULL)
            throw std::runtime_error("Invalid SST magic");
        if (f_size > 0 && f_offset + f_size <= file_size_ - 32)
        {
            filter_data_ = data + f_offset;
            filter_size_ = f_size;
        }

        uint32_t index_size = 0;
        std::memcpy(&index_size, data + meta_offset_, 4);
        index_.resize(index_size);
        size_t pos = meta_offset_ + 4;
        for (uint32_t i = 0; i < index_size; ++i)
        {
            if (pos + 4 + 8 + sizeof(KeyType) > file_size_)
                break;
            uint32_t k_len = 0;
            std::memcpy(&k_len, data + pos, 4);
            pos += 4;
            uint64_t offset = 0;
            std::memcpy(&offset, data + pos, 8);
            pos += 8;
            KeyType key;
            std::memcpy(&key, data + pos, sizeof(KeyType));
            pos += sizeof(KeyType);
            index_[i] = {key, offset};
        }
    }

    ~SSTReader() = default;

    bool Get(const KeyType &key, PinnableSlice &out_val, uint64_t &out_seq)
    {
        // ✅ Prefix Bloom Filter 快速排除
        // 语义：返回 false 则一定不存在；返回 true 则可能存在（需后续精确验证）
        if (filter_size_ > 0 &&
            !BloomFilter::MayMatch(filter_data_, filter_size_, static_cast<uint64_t>(key)))
            return false;

        if (index_.empty())
            return false;
        auto it = std::upper_bound(index_.begin(), index_.end(), key,
                                   [](const KeyType &k, const auto &e)
                                   { return k < e.first; });
        if (it == index_.begin())
            return false;
        --it;

        uint64_t block_offset = it->second;
        auto block_data = BlockCache::instance().get(cache_ns_, file_id_, block_offset);
        if (!block_data)
        {
            block_data = DecompressBlockFromMmap(block_offset);
            BlockCache::instance().put(cache_ns_, file_id_, block_offset, block_data);
        }

        const uint8_t *blk = block_data->data();
        size_t orig_size = block_data->size();
        if (orig_size < 4)
            return false;
        uint32_t num_restarts = 0;
        std::memcpy(&num_restarts, blk + orig_size - 4, 4);
        if (num_restarts * 4 + 4 > orig_size)
            return false;
        const uint8_t *restarts_ptr = blk + orig_size - 4 - num_restarts * 4;
        uint32_t num_seqs = 0;
        std::memcpy(&num_seqs, restarts_ptr - 4, 4);
        const uint8_t *seqs_ptr = restarts_ptr - 4 - num_seqs * 8;
        size_t data_end = seqs_ptr - blk;

        int left = 0, right = static_cast<int>(num_restarts) - 1;
        uint32_t search_start = 0;
        while (left <= right)
        {
            int mid = left + (right - left) / 2;
            uint32_t rp_offset = 0;
            std::memcpy(&rp_offset, restarts_ptr + mid * 4, 4);
            uint32_t k_len = 0;
            std::memcpy(&k_len, blk + rp_offset, 4);
            KeyType rp_key;
            std::memcpy(&rp_key, blk + rp_offset + 4, sizeof(KeyType));
            if (rp_key < key)
            {
                search_start = rp_offset;
                left = mid + 1;
            }
            else if (rp_key > key)
            {
                right = mid - 1;
            }
            else
            {
                search_start = rp_offset;
                break;
            }
        }

        uint32_t entry_idx = search_start / ENTRY_SIZE;
        uint64_t best_seq = 0;
        bool found = false;
        size_t best_pos = 0;
        uint32_t best_vlen = 0;
        size_t pos = search_start;
        while (pos + ENTRY_SIZE <= data_end)
        {
            uint32_t k_len = 0, v_len = 0;
            std::memcpy(&k_len, blk + pos, 4);
            pos += 4;
            KeyType k;
            std::memcpy(&k, blk + pos, sizeof(KeyType));
            pos += sizeof(KeyType);
            std::memcpy(&v_len, blk + pos, 4);
            pos += 4;
            if (pos + v_len > data_end)
                break;
            if (k == key)
            {
                uint64_t s = 0;
                if (entry_idx < num_seqs)
                    std::memcpy(&s, seqs_ptr + entry_idx * 8, 8);
                if (!found || s > best_seq)
                {
                    best_seq = s;
                    best_pos = pos;
                    best_vlen = v_len;
                    found = true;
                }
            }
            else if (k > key)
                break;
            pos += v_len;
            entry_idx++;
        }

        if (found)
        {
            out_val.pin_external(reinterpret_cast<const char *>(blk + best_pos), best_vlen,
                                 std::static_pointer_cast<void>(block_data));
            out_seq = best_seq;
            return true;
        }
        return false;
    }

    bool Get(const KeyType &key, PinnableSlice &out_val)
    {
        uint64_t dummy;
        return Get(key, out_val, dummy);
    }
    std::unique_ptr<Iterator> NewIterator() { return std::make_unique<Iterator>(this, file_id_); }

private:
    std::shared_ptr<std::vector<uint8_t>> DecompressBlockFromMmap(uint64_t block_offset)
    {
        std::shared_ptr<void> holder = mmap_holder_;
        if (block_offset + 8 >= file_size_)
            throw std::runtime_error("Block offset out of bounds");
        const uint8_t *base = static_cast<const uint8_t *>(holder.get()) + block_offset;
        uint32_t orig_size = 0, comp_size = 0;
        std::memcpy(&orig_size, base, 4);
        std::memcpy(&comp_size, base + 4, 4);
        if (block_offset + 8 + comp_size + 5 > file_size_)
            throw std::runtime_error("Block data exceeds file size");
        uint8_t type = base[8 + comp_size];
        auto block_data = std::make_shared<std::vector<uint8_t>>(orig_size);
        if (type == BLOCK_TYPE_LZ4)
        {
            int dec = LZ4_decompress_safe(reinterpret_cast<const char *>(base + 8),
                                          reinterpret_cast<char *>(block_data->data()),
                                          comp_size, orig_size);
            if (dec < 0)
                throw std::runtime_error("LZ4 decompress failed");
        }
        else
        {
            std::memcpy(block_data->data(), base + 8, orig_size);
        }
        return block_data;
    }

    std::string filepath_;
    uint64_t file_id_;
    uint64_t cache_ns_ = 0; // ✅ BlockCache 命名空间
    std::shared_ptr<void> mmap_holder_;
    size_t file_size_ = 0;
    uint64_t meta_offset_ = 0;
    std::vector<std::pair<KeyType, uint64_t>> index_;
    const uint8_t *filter_data_ = nullptr;
    size_t filter_size_ = 0;

public:
    // ✅ 改动②：SST 迭代器实现统一迭代器接口，可被 MergeIterator 直接归并
    class Iterator : public KVStoreIterator<KeyType>
    {
    public:
        explicit Iterator(SSTReader *reader, uint64_t file_id) : reader_(reader), file_id_(file_id)
        {
            if (reader_->index_.empty())
            {
                valid_ = false;
                return;
            }
            LoadBlock(0);
            ParseNextEntry();
        }

        bool Valid() const override { return valid_; }
        KeyType key() const override { return current_key_; }
        uint64_t seq() const override { return current_seq_; }
        const PinnableSlice &value() const { return current_slice_; }

        DataHandle handle() const override { return ConsumeHandle(); }

        DataHandle ConsumeHandle() const
        {
            uint64_t raw = 0;
            if (current_slice_.size() == sizeof(uint64_t))
                std::memcpy(&raw, current_slice_.data(), sizeof(uint64_t));
            return DataHandle(raw);
        }

        void Next() override
        {
            if (valid_)
                ParseNextEntry();
        }

        void Seek(const KeyType &target) override
        {
            if (reader_->index_.empty())
            {
                valid_ = false;
                return;
            }

            // ✅ Seek 也可以利用 Prefix Bloom 跳过整个 SST
            // 如果目标 key 的前缀不在 filter 中，且当前 SST 的 min/max 范围也不包含该前缀
            // 则可以直接标记 invalid（由上层 LevelIterator 跳到下一个 SST）
            // 这里简化处理：仅在 Get 路径使用 Bloom，Seek 保持原有逻辑
            // （因为 Seek 通常需要定位到 >= target 的第一个 key，Bloom 无法直接回答这个问题）

            auto it = std::upper_bound(reader_->index_.begin(), reader_->index_.end(), target,
                                       [](const KeyType &k, const auto &e)
                                       { return k < e.first; });
            if (it == reader_->index_.begin())
                block_idx_ = 0;
            else
            {
                --it;
                block_idx_ = std::distance(reader_->index_.begin(), it);
            }

            LoadBlock(block_idx_);

            const uint8_t *blk = current_block_->data();
            uint32_t num_restarts = 0;
            std::memcpy(&num_restarts, blk + current_block_->size() - 4, 4);
            const uint8_t *restarts_ptr = blk + current_block_->size() - 4 - num_restarts * 4;

            int left = 0, right = static_cast<int>(num_restarts) - 1;
            uint32_t search_start = 0;
            while (left <= right)
            {
                int mid = left + (right - left) / 2;
                uint32_t rp_offset = 0;
                std::memcpy(&rp_offset, restarts_ptr + mid * 4, 4);
                uint32_t k_len = 0;
                std::memcpy(&k_len, blk + rp_offset, 4);
                KeyType rp_key;
                std::memcpy(&rp_key, blk + rp_offset + 4, sizeof(KeyType));
                if (rp_key < target)
                {
                    search_start = rp_offset;
                    left = mid + 1;
                }
                else if (rp_key > target)
                {
                    right = mid - 1;
                }
                else
                {
                    search_start = rp_offset;
                    break;
                }
            }

            pos_ = search_start;
            entry_idx_ = search_start / SSTReader::ENTRY_SIZE;

            ParseNextEntry();
            while (valid_ && current_key_ < target)
                ParseNextEntry();
        }

    private:
        void LoadBlock(size_t idx)
        {
            uint64_t offset = reader_->index_[idx].second;
            current_block_ = BlockCache::instance().get(reader_->cache_ns_, file_id_, offset);
            if (!current_block_)
            {
                current_block_ = reader_->DecompressBlockFromMmap(offset);
                BlockCache::instance().put(reader_->cache_ns_, file_id_, offset, current_block_);
            }
            pos_ = 0;
            entry_idx_ = 0;
            uint32_t num_restarts = 0;
            std::memcpy(&num_restarts, current_block_->data() + current_block_->size() - 4, 4);
            const uint8_t *restarts_ptr = current_block_->data() + current_block_->size() - 4 - num_restarts * 4;
            uint32_t num_seqs = 0;
            std::memcpy(&num_seqs, restarts_ptr - 4, 4);
            seqs_ptr_ = restarts_ptr - 4 - num_seqs * 8;
            num_seqs_ = num_seqs;
            data_end_ = seqs_ptr_ - current_block_->data();
        }

        void ParseNextEntry()
        {
            while (true)
            {
                if (pos_ + SSTReader::ENTRY_SIZE > data_end_)
                {
                    if (++block_idx_ >= reader_->index_.size())
                    {
                        valid_ = false;
                        return;
                    }
                    LoadBlock(block_idx_);
                    continue;
                }
                const uint8_t *blk = current_block_->data();
                uint32_t k_len = 0, v_len = 0;
                std::memcpy(&k_len, blk + pos_, 4);
                pos_ += 4;
                std::memcpy(&current_key_, blk + pos_, sizeof(KeyType));
                pos_ += sizeof(KeyType);
                std::memcpy(&v_len, blk + pos_, 4);
                pos_ += 4;
                if (pos_ + v_len > data_end_)
                {
                    valid_ = false;
                    return;
                }

                last_pinned_block_ = current_block_;
                current_slice_.Reset();
                current_slice_.pin_external(reinterpret_cast<const char *>(blk + pos_), v_len,
                                            std::static_pointer_cast<void>(last_pinned_block_));

                if (entry_idx_ < num_seqs_)
                    std::memcpy(&current_seq_, seqs_ptr_ + entry_idx_ * 8, 8);
                else
                    current_seq_ = 0;

                pos_ += v_len;
                entry_idx_++;
                valid_ = true;
                return;
            }
        }

        SSTReader *reader_;
        uint64_t file_id_;
        size_t block_idx_ = 0, pos_ = 0, data_end_ = 0;
        size_t entry_idx_ = 0;
        uint32_t num_seqs_ = 0;
        const uint8_t *seqs_ptr_ = nullptr;
        std::shared_ptr<std::vector<uint8_t>> current_block_;
        std::shared_ptr<std::vector<uint8_t>> last_pinned_block_;
        KeyType current_key_;
        uint64_t current_seq_ = 0;
        PinnableSlice current_slice_;
        bool valid_ = true;
    };
};