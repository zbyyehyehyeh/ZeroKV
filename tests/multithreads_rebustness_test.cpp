// ============================================================
// ZeroKV 多线程崩溃注入测试（multithreaded crash injection）
//
// 与单写线程崩溃测试的区别：本测试用多个写线程并发写，在随机时刻
// kill -9，验证"已确认提交"的 key 全部可恢复。
//
// 关键语义（group commit 之后尤其重要）：
//   "已提交" = WAL 已 flush + fsync，且 commit 清单已 fsync。
//   Put 返回 ≠ 已提交（记录可能还在 group buffer 里，kill -9 会丢）。
//
// 用法：
//   ./zerokv_mt_robustness                 # 跑自包含 fork + kill -9 测试
// ============================================================
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include "DB.hpp"
#include "DataHandle.h"
#include "EpochManager.hpp"
#include "LockFreeSkipList.hpp"
#include "MemPool.hpp"

using namespace kvstore;

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond) do { \
    if (!(cond)) { std::printf("    [FAIL] %s:%d  %s\n", __FILE__, __LINE__, #cond); ++g_failed; } \
    else ++g_passed; \
} while (0)

// 每个写线程用它初始化 TLS 内存池 + EpochManager（与现有测试一致）
struct ThreadLocalGuard {
    using NodePool = TlsMemPoolManager<LockFreeSkipList<uint64_t>::NodeType>;
    ThreadLocalGuard() { NodePool::instance().InitThreadLocalPool(); }
    ~ThreadLocalGuard() {
        EpochManager::instance().try_reclaim();
        NodePool::instance().ReleaseThreadLocalPool();
    }
};

static std::string temp_dir(const std::string& name) {
    static std::atomic<int> counter{0};
    std::string dir = "/tmp/zerokv_mt_" + name + "_" + std::to_string(::getpid()) +
                      "_" + std::to_string(counter.fetch_add(1));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static std::string make_value(int tid, int i) {
    return "mt_" + std::to_string(tid) + "_" + std::to_string(i);
}

// ============ 自包含 fork + kill -9 测试 ============
static bool test_multithread_crash() {
    const int T = 4;            // 写线程数
    const int K = 200000;       // 每线程写 K 条（足够大，保证 kill 落在写中间）
    std::string dir = temp_dir("crash");

    pid_t pid = ::fork();
    CHECK(pid >= 0);
    if (pid < 0) { std::filesystem::remove_all(dir); return false; }

    if (pid == 0) {
        // ===== 子进程：T 线程并发写，协调线程负责 SyncWAL + 提交清单 =====
        DB db(dir);
        std::atomic<uint64_t> written[T];
        for (int t = 0; t < T; ++t) written[t].store(0);

        std::vector<std::thread> writers;
        for (int t = 0; t < T; ++t) {
            writers.emplace_back([&, t]() {
                ThreadLocalGuard guard;   // 初始化本线程 TLS
                for (int i = 0; i < K; ++i) {
                    uint64_t key = (uint64_t)t * K + (uint64_t)i;
                    std::string v = make_value(t, i);
                    db.Put(key, v.data(), static_cast<uint32_t>(v.size()));
                    written[t].store(static_cast<uint64_t>(i + 1), std::memory_order_release);
                }
            });
        }

        // 协调线程：先快照进度 → SyncWAL → 写提交清单 → fsync 清单
        // 顺序保证：清单里声明的 key，一定已经被 SyncWAL 落到盘。
        std::string cpath = dir + "/commit";
        int cfd = ::open(cpath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (cfd < 0) _exit(1);

        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            uint64_t snap[T];
            bool all_done = true;
            for (int t = 0; t < T; ++t) {
                snap[t] = written[t].load(std::memory_order_acquire);
                if (snap[t] < (uint64_t)K) all_done = false;
            }

            db.SyncWAL();   // flush + fsync，确保快照内的 key 已落盘

            char line[128];
            int n = std::snprintf(line, sizeof(line), "%llu %llu %llu %llu\n",
                                  (unsigned long long)snap[0], (unsigned long long)snap[1],
                                  (unsigned long long)snap[2], (unsigned long long)snap[3]);
            ssize_t w = ::write(cfd, line, static_cast<size_t>(n));
            (void)w;
            ::fdatasync(cfd);   // 清单落盘

            if (all_done) break;
        }
        ::close(cfd);
        for (auto& th : writers) th.join();
        _exit(0);   // 不析构，模拟 kill -9
    }

    // ===== 父进程：随机延迟后 kill -9 =====
    std::mt19937 rng(static_cast<unsigned>(::getpid()));
    std::uniform_int_distribution<int> dist(300, 2000);   // ms
    std::this_thread::sleep_for(std::chrono::milliseconds(dist(rng)));
    ::kill(pid, SIGKILL);
    int status = 0;
    ::waitpid(pid, &status, 0);

    // ===== 校验：读最后一行"完整且可解析"的清单，验证其中所有 key 可恢复 =====
    bool ok = true;
    {
        DB db(dir);   // 重开触发恢复

        std::string cpath = dir + "/commit";
        std::ifstream in(cpath);
        if (!in.is_open()) {
            std::filesystem::remove_all(dir);
            std::printf("  [mt_crash] killed before first commit -> PASS\n");
            return true;
        }

        // 取最后一行能解析出 T 个数的完整清单（避开 kill 中断造成的半行）
        std::vector<unsigned long long> s;
        std::string line;
        while (std::getline(in, line)) {
            std::istringstream iss(line);
            std::vector<unsigned long long> cur;
            unsigned long long v;
            while (iss >> v) cur.push_back(v);
            if (cur.size() == (size_t)T) s = cur;   // 只有完整行才采用
        }
        in.close();

        if (s.empty()) {
            std::filesystem::remove_all(dir);
            std::printf("  [mt_crash] no complete commit line -> PASS\n");
            return true;
        }

        size_t total = 0, recovered = 0;
        for (int t = 0; t < T; ++t) {
            for (unsigned long long i = 0; i < s[t]; ++i) {
                ++total;
                uint64_t key = (uint64_t)t * K + i;
                std::string v, expect = make_value(t, static_cast<int>(i));
                if (db.Get(key, v) && v == expect) ++recovered;
            }
        }

        CHECK(recovered == total);
        std::printf("  [mt_crash] recovered %zu/%zu committed keys\n", recovered, total);
        ok = (recovered == total);
    }

    std::filesystem::remove_all(dir);
    return ok;
}

int main() {
    std::printf("==========================================================\n");
    std::printf(" ZeroKV Multithreaded Crash Injection Test\n");
    std::printf("==========================================================\n");

    test_multithread_crash();

    std::printf("----------------------------------------------------------\n");
    std::printf(" PASSED: %d  FAILED: %d\n", g_passed, g_failed);
    std::printf(" %s\n", (g_failed == 0) ? "🎉 multithread crash test passed." : "❌ failed.");
    return g_failed == 0 ? 0 : 1;
}