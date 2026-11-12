// clean_perf.cpp — 干净的写/读采样负载（供火焰图用）
//
// 用法：
//   ./zerokv_clean_perf write [秒数]   纯写：预热后循环写 inline + VLog
//   ./zerokv_clean_perf read  [秒数]   纯读：落盘 + drop cache 后循环点查 + 范围扫描
//
// 两个"自伤"已修：
//   ① 时长判断不再每轮调 Clock::now()（每 4096 次才查一次时钟）
//   ② 读不再走 Get(拷贝版)，改走 GetZeroCopy + 触碰 1 字节（触发 page-in、防 DCE）
//
// 配合 perf：
//   sudo perf record -g --call-graph fp -F 99 ./build_prof/zerokv_clean_perf write
//   sudo perf script | ~/FlameGraph/stackcollapse-perf.pl | ~/FlameGraph/flamegraph.pl > write.svg
//
#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <thread>
#include <cstdlib>
#include <unistd.h>

#include "DB.hpp"
#include "EpochManager.hpp"

using namespace kvstore;
using Clock = std::chrono::steady_clock;

static const size_t N = 200000;              // setup 写入量
static const char   INLINE_VAL[] = "abcde";  // 5 字节，走 inline
static const std::string VLOG_VAL(1024, 'x'); // 1KB，走 ValueLog

void safe_remove_all(const std::string& path) {
    for (int i = 0; i < 5; ++i) {
        try {
            if (std::filesystem::exists(path)) std::filesystem::remove_all(path);
            return;
        } catch (...) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    }
}

bool is_root() { return geteuid() == 0; }

void drop_caches() {
    if (!is_root()) {
        std::cout << "  [Info] not root, skip drop_caches\n";
        return;
    }
    system("sync");
    system("echo 3 > /proc/sys/vm/drop_caches 2>/dev/null");
}

// 零拷贝读 + 触碰首字节（触发 mmap page-in、防止死代码消除），不拷贝整段
static inline bool zget(DB& db, uint64_t k) {
    PinnableSlice s;
    if (!db.GetZeroCopy(k, s)) return false;
    if (s.size() > 0) { volatile char c = s.data()[0]; (void)c; }
    return true;
}

int main(int argc, char** argv) {
    std::string mode = (argc > 1) ? argv[1] : "write";
    int dur = (argc > 2) ? std::atoi(argv[2]) : 20;
    const std::string dir = "/tmp/zerokv_clean_perf";
    std::mt19937_64 rng(42);

    if (mode == "write") {
        safe_remove_all(dir);
        std::filesystem::create_directories(dir);
        {
            DB db(dir);
            for (size_t i = 0; i < N; ++i) db.Put(i, INLINE_VAL, 5);
            db.SyncWAL();
            db.WaitForBackgroundJobs();

            std::cout << "== write path, measuring " << dur << "s ...\n";
            size_t ops = 0;
            auto t0 = Clock::now();
            for (;;) {
                uint64_t k = rng() % N;
                if ((k & 1) == 0) db.Put(k, INLINE_VAL, 5);
                else              db.Put(k, VLOG_VAL.data(), VLOG_VAL.size());
                ++ops;
                if ((ops & 4095) == 0 &&   // 每 4096 次才查时钟，避免 clock_gettime 污染
                    std::chrono::duration<double>(Clock::now() - t0).count() >= dur) break;
            }
            std::cout << "  wrote " << ops << " ops\n";
        }
    } else {  // read
        safe_remove_all(dir);
        std::filesystem::create_directories(dir);
        {
            DB db(dir);
            for (size_t i = 0; i < N; ++i) {
                if ((i & 1) == 0) db.Put(i, INLINE_VAL, 5);
                else              db.Put(i, VLOG_VAL.data(), VLOG_VAL.size());
            }
            db.SyncWAL();
            db.WaitForBackgroundJobs();
        }

        drop_caches();

        {
            DB db(dir);
            std::vector<uint64_t> keys(N);
            for (size_t i = 0; i < N; ++i) keys[i] = i;

            std::cout << "== cold read path, measuring " << dur << "s ...\n";
            size_t ops = 0;
            auto t0 = Clock::now();
            for (;;) {
                uint64_t k = keys[rng() % N];
                zget(db, k);
                if ((k & 255) == 0) {          // 少量范围扫描，暴露 LZ4 解压 + 跨块
                    auto it = db.NewIterator();
                    it->Seek(k);
                    int c = 0;
                    while (it->Valid() && c < 100) { std::string vv; it->GetValue(vv); it->Next(); ++c; }
                }
                ++ops;
                if ((ops & 4095) == 0 &&
                    std::chrono::duration<double>(Clock::now() - t0).count() >= dur) break;
            }
            std::cout << "  read " << ops << " ops\n";
        }
    }

    safe_remove_all(dir);
    std::cout << "done\n";
    return 0;
}