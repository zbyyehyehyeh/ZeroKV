#pragma once
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>

#include "ValueLog.hpp"

namespace kvstore
{
    using VLogRewriteMap = std::unordered_map<uint64_t, uint64_t>;
    using LatestHandleChecker = std::function<bool(uint64_t, uint64_t &)>;

    class ValueLogGC
    {
    public:
        explicit ValueLogGC(ValueLog *vlog) : vlog_(vlog) {}

        VLogRewriteMap RunOnce(uint32_t target_fid, LatestHandleChecker checker)
        {
            VLogRewriteMap rewrite_map;
            if (!vlog_ || target_fid == UINT32_MAX)
                return rewrite_map;

            uint32_t scan_offset = 0;
            uint64_t records_scanned = 0;
            uint64_t records_rewritten = 0;
            uint64_t records_skipped_deleted = 0;
            uint64_t records_skipped_newer = 0;

            while (true)
            {
                uint64_t key;
                PinnableSlice raw_value;
                uint32_t total_len = 0;

                if (!vlog_->ReadRawForGC(target_fid, scan_offset, key, raw_value, total_len))
                    break;

                records_scanned++;
                bool should_rewrite = false;

                if (checker)
                {
                    uint64_t latest_raw = 0;
                    bool exists = checker(key, latest_raw);
                    
                    if (exists)
                    {
                        DataHandle latest_dh(latest_raw);
                        // 仅当最新版本仍指向当前文件的当前offset时才rewrite
                        if (latest_dh.is_vlog() && 
                            latest_dh.vlog_file_id() == target_fid &&
                            latest_dh.vlog_offset() == scan_offset)
                        {
                            should_rewrite = true;
                        }
                        else
                        {
                            records_skipped_newer++;
                        }
                    }
                    else
                    {
                        records_skipped_deleted++;
                    }
                }

                if (should_rewrite)
                {
                    std::string safe_val(raw_value.data(), raw_value.size());
                    DataHandle new_dh = vlog_->AppendForGC(key, safe_val.data(), safe_val.size());
                    rewrite_map[key] = new_dh.raw;
                    records_rewritten++;
                }

                scan_offset += total_len;
            }

            if (!rewrite_map.empty())
                vlog_->Sync();

            return rewrite_map;
        }

    private:
        ValueLog *vlog_;
    };

} // namespace kvstore