#pragma once
#include <vector>
#include <cstdint>
#include <cstddef>
#include <algorithm>

class BloomFilter {
public:
    static constexpr int kBitsPerKey = 10;
    static constexpr int kNumProbes = 7;
    // ✅ 新增：定义前缀掩码，取 uint64_t 的高 32 位作为 Prefix
    // 对于 KV 分离的 Meta 层，高位通常包含 UserKey Hash 或 Partition ID，区分度足够
    static constexpr uint64_t PREFIX_MASK = 0xFFFFFFFF00000000ULL;
    static constexpr int PREFIX_SHIFT = 32;

    static inline uint64_t Hash64(uint64_t key) {
        key ^= key >> 33;
        key *= 0xff51afd7ed558ccdULL;
        key ^= key >> 33;
        key *= 0xc4ceb9fe1a85ec53ULL;
        key ^= key >> 33;
        return key;
    }

    // ✅ 新增：提取前缀
    static inline uint64_t ExtractPrefix(uint64_t key) {
        return key & PREFIX_MASK;
    }

    // ✅ 修改：构建时只插入前缀，大幅降低重复插入，提升缓存局部性
    static std::vector<uint8_t> BuildFilter(const std::vector<uint64_t>& keys) {
        if (keys.empty()) return {};

        // 提取并去重前缀，避免相同前缀重复设置 bit
        std::vector<uint64_t> prefixes;
        prefixes.reserve(keys.size());
        for (uint64_t key : keys) {
            prefixes.push_back(ExtractPrefix(key));
        }
        std::sort(prefixes.begin(), prefixes.end());
        auto last = std::unique(prefixes.begin(), prefixes.end());
        prefixes.erase(last, prefixes.end());

        size_t bits = prefixes.size() * kBitsPerKey;
        if (bits < 64) bits = 64;
        
        size_t bytes = (bits + 7) / 8;
        bits = bytes * 8; 

        std::vector<uint8_t> filter(bytes, 0);

        for (uint64_t prefix : prefixes) {
            uint64_t h = Hash64(prefix);
            uint32_t h1 = static_cast<uint32_t>(h);
            uint32_t h2 = static_cast<uint32_t>(h >> 32);

            for (int i = 0; i < kNumProbes; ++i) {
                uint32_t bitpos = (h1 + i * h2) % bits;
                filter[bitpos / 8] |= (1 << (bitpos % 8));
            }
        }
        return filter;
    }

    // ✅ 修改：查询时先提取前缀再匹配
    static inline bool MayMatch(const uint8_t* filter_data, size_t filter_size, uint64_t key) {
        if (filter_size == 0 || filter_data == nullptr) return true; 

        size_t bits = filter_size * 8;
        if (bits == 0) return true;
        
        // 关键：用前缀去查，而不是完整 key
        uint64_t prefix = ExtractPrefix(key);
        uint64_t h = Hash64(prefix);
        uint32_t h1 = static_cast<uint32_t>(h);
        uint32_t h2 = static_cast<uint32_t>(h >> 32);

        for (int i = 0; i < kNumProbes; ++i) {
            uint32_t bitpos = (h1 + i * h2) % bits;
            if ((filter_data[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
                return false; 
            }
        }
        return true; 
    }
};