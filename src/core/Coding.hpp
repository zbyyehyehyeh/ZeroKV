#pragma once
#include <cstdint>
#include <cstring>
#include <string>

// --- 解码 (Decode) ---
inline uint32_t DecodeFixed32(const char* ptr) {
    uint32_t result;
    std::memcpy(&result, ptr, sizeof(result));
    return result;
}
inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result;
    std::memcpy(&result, ptr, sizeof(result));
    return result;
}

// --- 编码 (Encode) ---
inline void EncodeFixed32(char* buf, uint32_t value) {
    std::memcpy(buf, &value, sizeof(value));
}
inline void EncodeFixed64(char* buf, uint64_t value) {
    std::memcpy(buf, &value, sizeof(value));
}

// --- 追加编码 (用于 std::string / std::vector) ---
inline void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
}