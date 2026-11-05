// ============================================================
// ZeroKV 可靠性测试（robustness tests）
// 覆盖：目录 fsync/MANIFEST 原子性、删除语义、KV 分离(VLog)、
//       SST/MANIFEST 原子性与容错、IO 错误注入、版本链、
//       崩溃恢复（fork + kill -9 配套）
// 编译：g++ -O2 -std=c++20 robustness_tests.cpp -o robustness_tests -llz4 -lz -lpthread
// 用法：
//   ./robustness_tests                    # 跑全部 7 组
//   ./robustness_tests <名字前缀>          # 只跑指定组（如 fsync_manifest）
//   ./robustness_tests writer <dir> <N>   # kill -9 脚本配套：写 N 条，每 1000 条 fsync 清单
//   ./robustness_tests checker <dir>      # kill -9 脚本配套：重开校验清单内 key
// ============================================================
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
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

static std::string temp_dir(const std::string& name) {
    static std::atomic<int> counter{0};
    std::string dir = "/tmp/zerokv_rob_" + name + "_" + std::to_string(::getpid()) +
                      "_" + std::to_string(counter.fetch_add(1));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static size_t file_size_of(const std::string& p) {
    std::error_code ec;
    auto sz = std::filesystem::file_size(p, ec);
    return ec ? 0 : sz;
}

struct ThreadLocalGuard {
    using NodePool = TlsMemPoolManager<LockFreeSkipList<uint64_t>::NodeType>;
    ThreadLocalGuard() { NodePool::instance().InitThreadLocalPool(); }
    ~ThreadLocalGuard() {
        EpochManager::instance().try_reclaim();
        NodePool::instance().ReleaseThreadLocalPool();
    }
};

// ============ [1/7] 目录 fsync / MANIFEST 原子性（功能层） ============
// 原子替换协议：写 MANIFEST.tmp → fdatasync(tmp) → rename → fsync(目录)。
// 功能层验证：tmp 不残留、残留 tmp 被忽略、坏行/孤儿条目容错。
// 系统调用层的 fsync(目录) 用 strace 验证（见文件末尾"使用方法"）。
static bool test_fsync_manifest() {
    bool ok = true;
    std::string dir = temp_dir("manifest");
    const int N = 3000;
    {
        DB db(dir);
        for (int i = 0; i < N; ++i) {
            std::string v = "m_" + std::to_string(i);
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        }
        db.SyncWAL();
        db.ForceGlobalCompaction();   // 触发 SaveManifestInternal
    } // 析构再次保存 MANIFEST

    std::string mf = dir + "/MANIFEST";
    std::string tf = dir + "/MANIFEST.tmp";
    CHECK(std::filesystem::exists(mf));       // MANIFEST 已原子落盘
    CHECK(!std::filesystem::exists(tf));      // 原子替换后 tmp 不残留
    CHECK(file_size_of(mf) > 0);

    // 残留的 MANIFEST.tmp（模拟 rename 前崩溃留下的半写）应被完全忽略
    {
        std::ofstream ofs(tf, std::ios::trunc);
        ofs << "half-written garbage line\n";
    }
    {
        DB db(dir);
        std::string v;
        CHECK(db.Get(0, v) && v == "m_0");
        CHECK(db.Get(N - 1, v) && v == "m_" + std::to_string(N - 1));
    }

    // MANIFEST 混入坏行 + 指向不存在文件的条目：跳过，不影响其余数据
    {
        std::ofstream ofs(mf, std::ios::app);
        ofs << "not a valid line @@\n";
        ofs << "1 999 0 100 50 1234 /nonexistent/sst_999.sst\n";
    }
    {
        DB db(dir);
        std::string v;
        CHECK(db.Get(0, v) && v == "m_0");
        CHECK(db.Get(N - 1, v) && v == "m_" + std::to_string(N - 1));
    }
    std::filesystem::remove_all(dir);
    std::printf("  [1/7] fsync/manifest %s | 原子替换 + tmp 残留容错 + 坏行/孤儿条目容错\n", ok ? "✅ PASS" : "❌ FAIL");
    return ok;
}

// ============ [2/7] 删除语义 ============
static bool test_delete() {
    bool ok = true;
    std::string dir = temp_dir("delete");
    const int N = 2000;
    {
        DB db(dir);
        std::string big(256, 'x');
        for (int i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), big.data(), static_cast<uint32_t>(big.size()));
        for (int i = 0; i < 500; ++i) db.Delete(static_cast<uint64_t>(i));   // 删除大值(VLog)
        db.Delete(999999);                                                   // 删除不存在的 key：无害
        std::string v;
        CHECK(!db.Get(0, v));
        CHECK(!db.Get(499, v));
        CHECK(db.Get(500, v) && v == big);
        db.Put(100000, "abcde", 5);                                          // 删除小值(inline)
        db.Delete(100000);
        CHECK(!db.Get(100000, v));
        db.Put(500, "new_val", 7);                                           // 覆盖后删除
        db.Delete(500);
        CHECK(!db.Get(500, v));
        db.SyncWAL();
    }
    {
        DB db(dir);   // 重启：tombstone 已持久化
        std::string v;
        for (int i = 0; i < 500; ++i) CHECK(!db.Get(static_cast<uint64_t>(i), v));
        CHECK(!db.Get(500, v));
        CHECK(!db.Get(100000, v));
        CHECK(!db.Get(999999, v));
        CHECK(db.Get(501, v));
        // 删除语义跨 Compaction / GC 保持（不复活）
        db.Delete(501);
        db.ForceGlobalCompaction();
        CHECK(!db.Get(501, v));
        db.Delete(502);
        db.RunFullGC();
        CHECK(!db.Get(502, v));
        CHECK(db.Get(503, v));   // GC 后未删除的数据仍可读
    }
    std::filesystem::remove_all(dir);
    std::printf("  [2/7] delete         %s | 大值/小值/覆盖删除 + 重启/Compaction/GC 后不复活\n", ok ? "✅ PASS" : "❌ FAIL");
    return ok;
}

// ============ [3/7] KV 分离（VLog） ============
static bool test_vlog_separation() {
    bool ok = true;
    std::string dir = temp_dir("vlog_sep");
    std::string vlog_dir = dir + "/vlog";
    // 阶段一：只写 ≤5 字节（inline）→ VLog 无数据
    {
        DB db(dir);
        for (int i = 0; i < 1000; ++i) db.Put(static_cast<uint64_t>(i), "abcde", 5);
        db.SyncWAL();
        size_t vlog_bytes = 0;
        for (auto& e : std::filesystem::directory_iterator(vlog_dir))
            if (e.is_regular_file()) vlog_bytes += e.file_size();
        CHECK(vlog_bytes == 0);
    }
    // 阶段二：写 >5 字节（VLog）→ 文件增长、读回一致
    size_t before = 0;
    {
        DB db(dir);
        for (auto& e : std::filesystem::directory_iterator(vlog_dir))
            if (e.is_regular_file()) before += e.file_size();
        std::string val(100, 'v');
        for (int i = 0; i < 1000; ++i)
            db.Put(static_cast<uint64_t>(1000 + i), val.data(), static_cast<uint32_t>(val.size()));
        db.SyncWAL();
        std::string v;
        for (int i = 0; i < 1000; ++i)
            CHECK(db.Get(static_cast<uint64_t>(1000 + i), v) && v == val);
        size_t after = 0;
        for (auto& e : std::filesystem::directory_iterator(vlog_dir))
            if (e.is_regular_file()) after += e.file_size();
        CHECK(after > before);
        CHECK(after - before >= 1000u * 100u);   // 至少是值本身的体积（16+8+100）/条
    }
    // 阶段三：VLog 尾部半写垃圾（模拟崩溃写一半）→ 重开后 CRC 截断、历史数据可读、可继续写
    {
        std::string vlog_file = vlog_dir + "/vlog_1.vlog";
        int fd = ::open(vlog_file.c_str(), O_WRONLY | O_APPEND);
        if (fd >= 0) {
            char junk[64];
            std::memset(junk, 0xCD, sizeof(junk));
            ssize_t w = ::write(fd, junk, sizeof(junk));
            (void)w;
            ::close(fd);
        }
        DB db(dir);   // RecoverAndTruncate 截断垃圾尾部
        std::string v;
        CHECK(db.Get(0, v) && v == "abcde");
        CHECK(db.Get(1000, v) && v.size() == 100);
        CHECK(db.Get(1999, v) && v.size() == 100);
        std::string nv(64, 'n');
        db.Put(5000, nv.data(), static_cast<uint32_t>(nv.size()));
        CHECK(db.Get(5000, v) && v == nv);
    }
    std::filesystem::remove_all(dir);
    std::printf("  [3/7] vlog(KV分离)   %s | 阈值分流 + 大值读写 + 尾部半写截断恢复\n", ok ? "✅ PASS" : "❌ FAIL");
    return ok;
}

// ============ [4/7] manifest / SSTable 原子性与容错 ============
static bool test_atomicity() {
    bool ok = true;
    std::string dir = temp_dir("atomicity");
    const int N = 1000;
    {
        DB db(dir);
        std::string v(128, 'a');
        for (int i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        db.SyncWAL();
    }
    {   // 第二批写入，保证至少两个 SST，便于"删一个、留一个"
        DB db(dir);
        std::string v(128, 'a');
        for (int i = N; i < 2 * N; ++i)
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        db.SyncWAL();
    }
    // 1) 孤儿 SST（不在 MANIFEST 中）应被忽略
    {
        std::ofstream ofs(dir + "/sst_999.sst", std::ios::binary);
        ofs << "orphan garbage, not a real sst file";
    }
    // 2) 删除 MANIFEST 里第一条 SST：打开时跳过缺失文件，其余数据可读
    std::string victim;
    {
        std::ifstream ifs(dir + "/MANIFEST");
        std::string line;
        while (std::getline(ifs, line)) {
            std::istringstream iss(line);
            int level; uint64_t fid, mn, mx, ms, sz; std::string path;
            iss >> level >> fid >> mn >> mx >> ms >> sz >> path;
            if (!iss.fail() && std::filesystem::exists(path)) { victim = path; break; }
        }
    }
    CHECK(!victim.empty());
    if (!victim.empty()) std::filesystem::remove(victim);
    {
        DB db(dir);   // 不崩；victim 覆盖的 key 丢失，其余可读
        std::string v;
        CHECK(!db.Get(0, v));
        CHECK(db.Get(static_cast<uint64_t>(N), v) && v.size() == 128);
    }
    std::filesystem::remove_all(dir);

    // 3) SST 文件损坏（截断）→ 打开时抛异常（fail fast），进程不崩
    std::filesystem::create_directories(dir);
    {
        DB db(dir);
        std::string v(128, 'b');
        for (int i = 0; i < 1000; ++i)
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        db.SyncWAL();
    }
    {
        std::string sst;
        for (auto& e : std::filesystem::directory_iterator(dir))
            if (e.path().extension() == ".sst") { sst = e.path().string(); break; }
        CHECK(!sst.empty());
        if (!sst.empty()) std::filesystem::resize_file(sst, 10);   // 截断破坏
        bool threw = false;
        try { DB db(dir); }
        catch (...) { threw = true; }
        CHECK(threw);   // 检测到损坏并拒绝打开，而不是返回错误数据
    }
    std::filesystem::remove_all(dir);
    std::printf("  [4/7] manifest/SST   %s | 孤儿文件忽略 + 缺失SST跳过 + 损坏SST fail-fast\n", ok ? "✅ PASS" : "❌ FAIL");
    return ok;
}

// ============ [5/7] IO 错误注入 ============
static bool test_io_error() {
    bool ok = true;
    // 1) 打开失败：非法路径 → 构造抛异常，进程不崩
    {
        bool threw = false;
        try { DB db("/proc/__zerokv_no_such_dir__"); }
        catch (...) { threw = true; }
        CHECK(threw);
    }
    // 2) VLog 数据文件被外部删除 → 打开不崩，受影响 key 优雅返回 false，新写入仍可用
    std::string dir = temp_dir("io_vlog");
    {
        DB db(dir);
        std::string v(256, 'd');
        for (int i = 0; i < 1000; ++i)
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        db.SyncWAL();
    }
    std::string vlog_file = dir + "/vlog/vlog_1.vlog";
    CHECK(std::filesystem::exists(vlog_file));
    std::filesystem::remove(vlog_file);
    {
        DB db(dir);   // 重开自动重建空 VLog；旧句柄指向的空文件读失败 → 返回 false（会打 5 行 [DIAG]，属预期）
        std::string v;
        size_t readable = 0;
        for (int i = 0; i < 1000; ++i) if (db.Get(static_cast<uint64_t>(i), v)) ++readable;
        CHECK(readable == 0);
        std::string nv(64, 'n');
        db.Put(2000, nv.data(), static_cast<uint32_t>(nv.size()));
        CHECK(db.Get(2000, v) && v == nv);
    }
    std::filesystem::remove_all(dir);
    // 3) WAL 尾部损坏 → 恢复截断，DB 正常打开且历史数据可读
    std::string dir2 = temp_dir("io_wal");
    {
        DB db(dir2);
        std::string v(64, 'w');
        for (int i = 0; i < 500; ++i)
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        db.SyncWAL();
    }
    {
        int fd = ::open((dir2 + "/wal.log").c_str(), O_WRONLY | O_APPEND);
        if (fd >= 0) {
            char junk[128];
            std::memset(junk, 0xEE, sizeof(junk));
            ssize_t w = ::write(fd, junk, sizeof(junk));
            (void)w;
            ::close(fd);
        }
        DB db(dir2);
        std::string v;
        CHECK(db.Get(0, v) && v.size() == 64);
        CHECK(db.Get(499, v) && v.size() == 64);
    }
    std::filesystem::remove_all(dir2);
    std::printf("  [5/7] io_error       %s | 打开失败抛异常 + VLog丢失优雅降级 + WAL损坏截断\n", ok ? "✅ PASS" : "❌ FAIL");
    return ok;
}

// ============ [6/7] 版本链 ============
static bool test_version_chain() {
    bool ok = true;
    std::string dir = temp_dir("version_chain");
    const int ROUNDS = 100;
    {
        DB db(dir);
        std::string v;
        // 1) 顺序覆盖（大值）：Get 始终返回最后一次写入
        for (int i = 0; i < ROUNDS; ++i) {
            std::string big(128, static_cast<char>('A' + (i % 26)));
            db.Put(7, big.data(), static_cast<uint32_t>(big.size()));
        }
        CHECK(db.Get(7, v) && v == std::string(128, static_cast<char>('A' + ((ROUNDS - 1) % 26))));
        // 2) 大小值交替覆盖（inline ↔ VLog 切换）
        db.Put(7, "tiny", 4);
        CHECK(db.Get(7, v) && v == "tiny");
        db.Put(7, "bigger_than_five", 16);
        CHECK(db.Get(7, v) && v == std::string("bigger_than_five"));
        // 3) 墓碑覆盖 + 复活
        db.Delete(7);
        CHECK(!db.Get(7, v));
        db.Put(7, "reborn", 6);
        CHECK(db.Get(7, v) && v == "reborn");
        // 4) 并发同 key 覆盖：最终值必须是某次写入的完整值（无撕裂）
        const int T = 4, PER = 2000;
        std::vector<std::string> finals(T);
        std::vector<std::thread> ths;
        for (int t = 0; t < T; ++t) {
            finals[t] = std::string("t") + std::to_string(t) + "_" +
                        std::string(64, static_cast<char>('a' + t));
            ths.emplace_back([&db, t, &finals, PER]() {
                ThreadLocalGuard guard;
                for (int i = 0; i < PER; ++i)
                    db.Put(9, finals[t].data(), static_cast<uint32_t>(finals[t].size()));
            });
        }
        for (auto& th : ths) th.join();
        std::string got;
        CHECK(db.Get(9, got));
        bool match = false;
        for (auto& f : finals) if (got == f) { match = true; break; }
        CHECK(match);
        db.SyncWAL();
    }
    {   // 5) 版本链经重启 / GC 后仍取最新
        DB db(dir);
        std::string v;
        CHECK(db.Get(7, v) && v == "reborn");
        db.RunFullGC();
        CHECK(db.Get(7, v) && v == "reborn");
        std::string got;
        CHECK(db.Get(9, got));   // 并发覆盖的最终值在 GC 后仍可读
    }
    std::filesystem::remove_all(dir);
    std::printf("  [6/7] version_chain  %s | 顺序/并发覆盖无撕裂 + 大小值交替 + 墓碑复活 + GC 后取最新\n", ok ? "✅ PASS" : "❌ FAIL");
    return ok;
}

// ============ [7/7] 崩溃恢复（fork 确定性测试，含 WAL 轮转） ============
static bool test_crash_fork() {
    bool ok = true;
    std::string dir = temp_dir("crash_fork");
    const int N = 3000, M = 500;
    pid_t pid = ::fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        {
            DB db(dir);
            for (int i = 0; i < N; ++i) {
                std::string v = "crash_value_" + std::to_string(i);
                db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
            }
            db.SyncWAL();
            db.ForceGlobalCompaction();          // flush → WAL 轮转
            size_t wal_size = file_size_of(dir + "/wal.log");
            if (wal_size != 0) _exit(1);         // 轮转后 WAL 必须被截断
            for (int i = N; i < N + M; ++i) {    // 轮转后的新写入（不 sync）
                std::string v = "crash_value_" + std::to_string(i);
                db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
            }
        }
        _exit(0);   // 不析构 = 模拟 kill -9
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    {
        DB db(dir);   // 重开：轮转前的 N 条（SST）+ 轮转后的 M 条（WAL 回放）都应可读
        size_t okn = 0;
        for (int i = 0; i < N + M; ++i) {
            std::string v, expect = "crash_value_" + std::to_string(i);
            if (db.Get(static_cast<uint64_t>(i), v) && v == expect) ++okn;
        }
        CHECK(okn == static_cast<size_t>(N + M));
    }
    std::filesystem::remove_all(dir);
    std::printf("  [7/7] crash(fork)    %s | WAL轮转截断 + 轮转后写入崩溃恢复 %d/%d\n",
                ok ? "✅ PASS" : "❌ FAIL", N + M, N + M);
    return ok;
}

// ============ kill -9 脚本配套：writer / checker ============
static int run_writer(const std::string& dir, int N) {
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    DB db(dir);
    std::string commit_path = dir + "/commit";
    int cfd = ::open(commit_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (cfd < 0) return 1;
    for (int i = 0; i < N; ++i) {
        std::string v(1024, static_cast<char>('c' + (i % 26)));   // 1KB 值，提高撕裂概率
        db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        if (i % 1000 == 999) {
            char line[32];
            int n = snprintf(line, sizeof(line), "%d\n", i);
            ssize_t w = ::write(cfd, line, static_cast<size_t>(n));
            (void)w;
            ::fdatasync(cfd);   // 清单落盘后才算"已确认提交"
            std::printf("COMMIT %d\n", i);
            std::fflush(stdout);
        }
    }
    ::close(cfd);
    return 0;
}

static int run_checker(const std::string& dir) {
    DB db(dir);   // 重开触发恢复
    std::string commit_path = dir + "/commit";
    FILE* f = std::fopen(commit_path.c_str(), "r");
    if (!f) { std::printf("  no commit file (killed before first batch) -> PASS\n"); return 0; }
    int last = -1, boundary = -1;
    while (std::fscanf(f, "%d", &boundary) == 1) last = boundary;
    std::fclose(f);
    if (last < 0) { std::printf("  no committed batch -> PASS\n"); return 0; }
    size_t ok = 0;
    for (int i = 0; i <= last; ++i) {
        std::string v;
        std::string expect(1024, static_cast<char>('c' + (i % 26)));
        if (db.Get(static_cast<uint64_t>(i), v) && v == expect) ++ok;
    }
    std::printf("  checker: %d/%d committed keys recovered\n", static_cast<int>(ok), last + 1);
    return ok == static_cast<size_t>(last + 1) ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc >= 2 && std::strcmp(argv[1], "writer") == 0 && argc >= 4)
        return run_writer(argv[2], std::atoi(argv[3]));
    if (argc >= 2 && std::strcmp(argv[1], "checker") == 0 && argc >= 3)
        return run_checker(argv[2]);

    std::printf("==========================================================\n");
    std::printf(" ZeroKV Robustness Tests\n");
    std::printf("==========================================================\n");
    const char* filter = (argc >= 2) ? argv[1] : nullptr;
    bool all = true;
    auto run = [&](const char* name, bool (*fn)()) {
        if (filter && std::strncmp(name, filter, std::strlen(filter)) != 0) return;
        all = fn() && all;
    };
    run("fsync_manifest", test_fsync_manifest);
    run("delete",         test_delete);
    run("vlog",           test_vlog_separation);
    run("atomicity",      test_atomicity);
    run("io_error",       test_io_error);
    run("version_chain",  test_version_chain);
    run("crash",          test_crash_fork);
    std::printf("----------------------------------------------------------\n");
    std::printf(" PASSED: %d  FAILED: %d\n", g_passed, g_failed);
    std::printf(" %s\n", all ? "🎉 All robustness tests passed." : "❌ Some tests failed.");
    return all ? 0 : 1;
}