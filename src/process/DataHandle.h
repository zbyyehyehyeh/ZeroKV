#pragma once
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <atomic>
#include <iostream>

enum class DataTier : uint8_t { EMPTY = 0, INLINE = 1, VLOG = 2, EXTERNAL = 3 };
enum class DataType : uint8_t { RAW = 0, INT64_ARRAY = 1, FLOAT64_ARRAY = 2, STRING = 3 };

struct alignas(8) DataHandle {
    uint64_t raw = 0;

    DataHandle() noexcept = default;
    explicit DataHandle(uint64_t r) noexcept : raw(r) {}

    // =====================================================================
    // 🏆 64-bit Handle 位域布局
    // Bits 63-62: Tier   (2 bits)
    // Bits 61-60: Type   (2 bits)
    // Bits 59-40: Size   (20 bits) -> Max 1MB
    // Bits 39-30: FileID (10 bits) -> Max 1023 files
    // Bits 29-0:  Offset (30 bits) -> Max 1GB
    // ⚠️ Inline Payload 使用低 40 bits (Bits 39-0)，最大 5 字节安全存储
    // =====================================================================
    
    static constexpr int SHIFT_TIER   = 62;
    static constexpr int SHIFT_TYPE   = 60;
    static constexpr int SHIFT_SIZE   = 40;
    static constexpr int SHIFT_FILEID = 30;
    static constexpr int SHIFT_OFFSET = 0;
    
    static constexpr uint64_t MASK_TIER   = 0x3ULL;         
    static constexpr uint64_t MASK_TYPE   = 0x3ULL;         
    static constexpr uint64_t MASK_SIZE   = 0xFFFFFULL;      // 20 bits
    static constexpr uint64_t MASK_FILEID = 0x3FFULL;        // 🔧 修复：10 bits = 0x3FF (max 1023)，原注释写0x3FFFFF是错误的
    static constexpr uint64_t MASK_OFFSET = 0x3FFFFFFFULL;   // 30 bits
    static constexpr uint64_t MASK_INLINE_PAYLOAD = 0xFFFFFFFFFFULL; // 40 bits (5 bytes)

    static DataHandle make_inline(DataType type, const void* src, uint32_t len) {
        if (len > 5) [[unlikely]] 
            throw std::invalid_argument("Inline value max 5 bytes (6 bytes requires External tier)");
        
        uint64_t payload = 0;
        if (len > 0 && src) std::memcpy(&payload, src, len);
        
        uint64_t r = (static_cast<uint64_t>(DataTier::INLINE) << SHIFT_TIER) |
                     (static_cast<uint64_t>(type) << SHIFT_TYPE) |
                     (static_cast<uint64_t>(len) << SHIFT_SIZE) |
                     (payload & MASK_INLINE_PAYLOAD);
        return DataHandle(r);
    }

    static DataHandle make_vlog(DataType type, uint32_t file_id, uint32_t offset, uint32_t len) {
        if (len > MASK_SIZE) [[unlikely]] 
            throw std::invalid_argument("Value size exceeds 1MB limit");
        if (file_id > MASK_FILEID) [[unlikely]] 
            throw std::invalid_argument("VLog FileID exceeds 1023 limit");
        if (offset > MASK_OFFSET) [[unlikely]] 
            throw std::invalid_argument("VLog Offset exceeds 1GB limit");

        uint64_t r = (static_cast<uint64_t>(DataTier::VLOG) << SHIFT_TIER) |
                     (static_cast<uint64_t>(type) << SHIFT_TYPE) |
                     (static_cast<uint64_t>(len) << SHIFT_SIZE) |
                     (static_cast<uint64_t>(file_id) << SHIFT_FILEID) |
                     (static_cast<uint64_t>(offset));
        return DataHandle(r);
    }

    static DataHandle make_tombstone() noexcept { return DataHandle(0); }

    // ==================== 解析 API ====================
    DataTier tier() const noexcept { return static_cast<DataTier>((raw >> SHIFT_TIER) & MASK_TIER); }
    DataType type() const noexcept { return static_cast<DataType>((raw >> SHIFT_TYPE) & MASK_TYPE); }
    uint32_t size() const noexcept { return static_cast<uint32_t>((raw >> SHIFT_SIZE) & MASK_SIZE); }
    
    bool is_inline() const noexcept { return tier() == DataTier::INLINE; }
    bool is_vlog() const noexcept { return tier() == DataTier::VLOG; }
    bool is_tombstone() const noexcept { return raw == 0; } 

    uint32_t vlog_file_id() const noexcept { return static_cast<uint32_t>((raw >> SHIFT_FILEID) & MASK_FILEID); }
    uint32_t vlog_offset() const noexcept { return static_cast<uint32_t>((raw >> SHIFT_OFFSET) & MASK_OFFSET); }

    void copy_inline_to(char* dst) const noexcept {
        if (!is_inline()) return;
        uint32_t len = size();
        if (len > 0 && len <= 5) {
            uint64_t payload = raw & MASK_INLINE_PAYLOAD;
            std::memcpy(dst, &payload, len);
        }
    }
};

static_assert(sizeof(DataHandle) == 8, "DataHandle must be exactly 8 bytes");
static_assert(std::atomic<DataHandle>::is_always_lock_free, "DataHandle atomic ops must be lock-free");
