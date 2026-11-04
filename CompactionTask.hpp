#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <atomic>

#include "SSTable.hpp"
#include "MergeIterator.hpp"
#include "DataHandle.h"

namespace kvstore {

struct DBSSTMeta; // Forward declaration

struct CompactionInput {
    std::vector<std::shared_ptr<SSTReader<uint64_t>>> sources;
    int target_level;
    bool is_bottom_level;
    uint64_t cache_ns = 0;
    std::function<void(uint64_t key, uint64_t old_raw)> handle_obsolete;
    std::function<uint64_t(uint64_t key, uint64_t old_raw)> handle_rewriter;
};

struct CompactionOutput {
    std::vector<DBSSTMeta> new_ssts;
    std::vector<std::string> deleted_files;
};

class CompactionTask {
public:
    static CompactionOutput Execute(
        const CompactionInput& input,
        const std::string& dir,
        std::atomic<uint64_t>& next_file_id,
        size_t target_sst_size);
};

} // namespace kvstore