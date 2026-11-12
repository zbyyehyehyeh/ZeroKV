// latency_tests.cpp — 长尾延迟基准（手动运行，不作为 CTest）
//
// 测量 p50 / p90 / p99 / p99.9 / p99.99，覆盖：
//   1. 顺序写：inline（≤5B） vs VLog（1KB）两条路径
//   2. 点查命中：inline vs VLog（1KB）
//   3. 点查未命中（Bloom 否定路径）
//   4. 冷缓存范围扫描（Seek + Next×100）
//
// 运行：./build/zerokv_latency      （冷读建议 sudo ./build/zerokv_latency）
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

static const size_t N = 500000;    // 每类 key 的写入量
static const size_t Q = 100000;    // 每项查询量

void safe_remove_all(const std::string& path) {
    for (int i = 0; i < 5; ++i) {
        try {
            if (std::filesystem::exists(path)) std::filesystem::remove_all(path);
            return;
        } catch (...) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    }
}

bool is_root() { return geteuid() == 0; }

void DropPageCache() {
    if (!is_root()) {
        std::cout << "  [Info] 非 root，跳过 drop_caches（冷读数据不准确）\n";
        return;
    }
    system("sync");
    system("echo 3 > /proc/sys/vm/drop_caches 2>/dev/null");
}

struct LatencyReport {
    double avg;      // us
    double p50, p90, p99, p999, p9999;
};

LatencyReport compute(std::vector<double> us) {
    std::sort(us.begin(), us.end());
    auto pct = [&](double p) {
        size_t i = (size_t)((us.size() - 1) * p);
        return us[i];
    };
    double sum = 0.0;
    for (double x : us) sum += x;
    return {sum / us.size(), pct(0.50), pct(0.90), pct(0.99), pct(0.999), pct(0.9999)};
}

void print_row(const char* name, const LatencyReport& r) {
    std::printf("  %-30s avg=%8.2fus  p50=%8.2f  p90=%8.2f  p99=%9.2f  p99.9=%10.2f  p99.99=%10.2f\n",
                name, r.avg, r.p50, r.p90, r.p99, r.p999, r.p9999);
}

// 对每个操作单独计时，收集 us 数组
template <typename Fn>
std::vector<double> collect(Fn&& op, size_t count) {
    std::vector<double> lat;
    lat.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        auto t0 = Clock::now();
        op();
        auto t1 = Clock::now();
        lat.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return lat;
}

int main() {
    const std::string TEST_DIR = "/tmp/zerokv_latency";
    safe_remove_all(TEST_DIR);
    std::filesystem::create_directories(TEST_DIR);

    std::mt19937_64 rng(42);

    // inline key 用 [0, N)，VLog key 用 [N, 2N)，miss key 用 [2N, 3N)
    const uint64_t INLINE_BASE = 0;
    const uint64_t VLOG_BASE   = N;
    const uint64_t MISS_BASE   = 2 * N;

    const char  inline_val[] = "abcde";       // 5 字节 = inline 边界
    std::string vlog_val(1024, 'x');          // 1KB = 走 VLog

    std::cout << "⏱  ZeroKV Latency Benchmark | N=" << N << " | Q=" << Q << "\n";
    std::cout << "   inline value = 5B (内联边界) | vlog value = 1KB\n";
    std::cout << "--------------------------------------------------------------\n";

    try {
        // ========== 1. 顺序写延迟（inline vs vlog） ==========
        std::cout << "[1/3] Sequential Write Latency\n";
        {
            DB db(TEST_DIR);

            auto inline_lat = collect([&]() { db.Put(INLINE_BASE + (rng() % N), inline_val, 5); }, N);
            print_row("write inline(5B)", compute(inline_lat));

            auto vlog_lat = collect([&]() { db.Put(VLOG_BASE + (rng() % N), vlog_val.data(), vlog_val.size()); }, N);
            print_row("write vlog(1KB)", compute(vlog_lat));

            db.SyncWAL();
            db.WaitForBackgroundJobs();
        }
        std::cout << "--------------------------------------------------------------\n";

        // ========== 2. 点查命中 + 未命中 ==========
        std::cout << "[2/3] Point Get Latency (hit & miss)\n";
        {
            DB db(TEST_DIR);
            // warmup
            for (int i = 0; i < 2000; ++i) {
                std::string v;
                db.Get(INLINE_BASE + (rng() % N), v);
                db.Get(VLOG_BASE + (rng() % N), v);
            }

            auto hit_inline = collect([&]() { PinnableSlice s; if (db.GetZeroCopy(INLINE_BASE + (rng() % N), s)) { volatile char c = s.data()[0]; (void)c; } }, Q);
            print_row("get hit inline(5B)", compute(hit_inline));

            auto hit_vlog = collect([&]() { PinnableSlice s; if (db.GetZeroCopy(VLOG_BASE + (rng() % N), s)) { volatile char c = s.data()[0]; (void)c; } }, Q);
            print_row("get hit vlog(1KB)", compute(hit_vlog));

            auto miss = collect([&]() { PinnableSlice s; if (db.GetZeroCopy(MISS_BASE + (rng() % N), s)) { volatile char c = s.data()[0]; (void)c; } }, Q);
            print_row("get miss(Bloom)", compute(miss));
        }
        std::cout << "--------------------------------------------------------------\n";

        // ========== 3. 冷缓存范围扫描 ==========
        std::cout << "[3/3] Range Scan Latency (cold, Seek + Next x100)\n";
        {
            DropPageCache();
            DB db(TEST_DIR);  // 新实例，清空 BlockCache

            std::vector<uint64_t> starts;
            starts.reserve(Q);
            std::uniform_int_distribution<size_t> dist(0, N - 200);
            for (size_t i = 0; i < Q; ++i) starts.push_back(INLINE_BASE + dist(rng));

            auto scan_lat = collect([&]() {
                auto it = db.NewIterator();
                it->Seek(starts[rng() % Q]);
                int c = 0;
                while (it->Valid() && c < 100) { std::string v; it->GetValue(v); it->Next(); ++c; }
            }, Q);
            print_row("range scan cold(Seek+Next100)", compute(scan_lat));
        }
        std::cout << "--------------------------------------------------------------\n";

        std::cout << "🎉 latency benchmark done\n";
    } catch (const std::exception& e) {
        std::cerr << "❌ failed: " << e.what() << "\n";
        safe_remove_all(TEST_DIR);
        return 1;
    }

    safe_remove_all(TEST_DIR);
    return 0;
}
