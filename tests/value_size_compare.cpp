// value_size_compare.cpp — 大小 value 详细对比（手动运行，不作为 CTest）
//
// 对一系列 value 大小（覆盖 inline → VLog 的 5B/6B 分界）分别测：
//   - 顺序写吞吐 + 写延迟 p99
//   - 点查命中吞吐 + 读延迟 p99
//   - 磁盘占用（展示 VLog 是否增长）
//
// 运行：./build/zerokv_value_compare
//
#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <thread>
#include <cstdlib>
#include <unistd.h>

#include "DB.hpp"
#include "EpochManager.hpp"

using namespace kvstore;
using Clock = std::chrono::high_resolution_clock;

static const size_t N      = 200000;  // 吞吐测试写入量
static const size_t LAT_N  = 50000;   // 延迟采样量

void safe_remove_all(const std::string& path) {
    for (int i = 0; i < 5; ++i) {
        try {
            if (std::filesystem::exists(path)) std::filesystem::remove_all(path);
            return;
        } catch (...) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    }
}

size_t dir_size(const std::string& dir) {
    size_t total = 0;
    if (!std::filesystem::exists(dir)) return 0;
    try {
        for (auto& e : std::filesystem::recursive_directory_iterator(dir))
            if (e.is_regular_file()) total += e.file_size();
    } catch (...) {}
    return total;
}

double pct(std::vector<double> us, double p) {
    std::sort(us.begin(), us.end());
    return us[(size_t)((us.size() - 1) * p)];
}

int main() {
    const std::string ROOT = "/tmp/zerokv_size_compare";
    safe_remove_all(ROOT);

    // 覆盖 inline→VLog 分界（≤5B inline，>5B vlog）
    const std::vector<size_t> SIZES = {1, 4, 5, 6, 8, 16, 64, 256, 1024};

    std::mt19937_64 rng(42);

    std::printf("%-8s %-7s %12s %10s %12s %10s %12s\n",
                "size", "path", "write ops/s", "write p99", "read QPS", "read p99", "disk KB");
    std::printf("%s\n", std::string(74, '-').c_str());

    for (size_t vsz : SIZES) {
        std::string dir = ROOT + "/s" + std::to_string(vsz);
        std::filesystem::create_directories(dir);

        std::string val(vsz, 'x');
        const bool is_inline = (vsz <= 5);

        // 预生成 key（保证顺序写 + 随机读可控）
        std::vector<uint64_t> keys(N);
        for (size_t i = 0; i < N; ++i) keys[i] = (uint64_t)i;

        // ---- 写吞吐 ----
        double write_ops = 0.0;
        {
            DB db(dir);
            auto t0 = Clock::now();
            for (size_t i = 0; i < N; ++i) db.Put(keys[i], val.data(), vsz);
            db.SyncWAL();
            db.WaitForBackgroundJobs();
            auto t1 = Clock::now();
            write_ops = N / std::chrono::duration<double>(t1 - t0).count();
        }

        // ---- 写延迟 p99（单独一轮采样）----
        std::vector<double> wlat;
        {
            DB db(dir);
            wlat.reserve(LAT_N);
            for (size_t i = 0; i < LAT_N; ++i) {
                uint64_t k = keys[rng() % N];
                auto t0 = Clock::now();
                db.Put(k, val.data(), vsz);
                wlat.push_back(std::chrono::duration<double, std::micro>(Clock::now() - t0).count());
            }
        }

        // ---- 读吞吐 + 读延迟 ----
        double read_qps = 0.0;
        double read_p99 = 0.0;
        {
            DB db(dir);
            // warmup
            for (int i = 0; i < 2000; ++i) { PinnableSlice s; if (db.GetZeroCopy(keys[rng() % N], s)) { volatile char c = s.data()[0]; (void)c; } }

            auto t0 = Clock::now();
            for (size_t i = 0; i < N; ++i) { PinnableSlice s; if (db.GetZeroCopy(keys[rng() % N], s)) { volatile char c = s.data()[0]; (void)c; } }
            auto t1 = Clock::now();
            read_qps = N / std::chrono::duration<double>(t1 - t0).count();

            std::vector<double> rlat;
            rlat.reserve(LAT_N);
            for (size_t i = 0; i < LAT_N; ++i) {
                auto a = Clock::now();
                PinnableSlice s; if (db.GetZeroCopy(keys[rng() % N], s)) { volatile char c = s.data()[0]; (void)c; }
                rlat.push_back(std::chrono::duration<double, std::micro>(Clock::now() - a).count());
            }
            read_p99 = pct(rlat, 0.99);
        }

        size_t disk_kb = dir_size(dir) / 1024;
        std::printf("%-8zu %-7s %12.0f %9.2fus %12.0f %9.2fus %12zu\n",
                    vsz, is_inline ? "inline" : "vlog",
                    write_ops, pct(wlat, 0.99),
                    read_qps, read_p99, disk_kb);
    }

    safe_remove_all(ROOT);
    std::cout << "\n🎉 value size comparison done\n";
    return 0;
}
