// ============================================================
// ZeroKV 单元测试（自包含最小框架，无第三方依赖）
// 随 CMake 一起构建（目标 zerokv_tests）；直接运行：./build/zerokv_tests
// 强烈建议分别用 ASan/UBSan 与 TSan 各跑一遍（见 .github/workflows/ci.yml）
// 注意：本项目只支持 Linux 环境编译
// ============================================================
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "BloomFilter.hpp"
#include "Coding.hpp"
#include "DB.hpp"
#include "DataHandle.h"
#include "EpochManager.hpp"
#include "LockFreeSkipList.hpp"
#include "MemPool.hpp"
#include "WAL.hpp"

using namespace kvstore;

static int g_passed = 0;
static int g_failed = 0;

#define CHECK_TRUE(cond)                                                       \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::printf("  [FAIL] %s:%d  %s\n", __FILE__, __LINE__, #cond);    \
            ++g_failed;                                                        \
        } else {                                                               \
            ++g_passed;                                                        \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b)                                                         \
    do {                                                                       \
        auto _va = (a);                                                        \
        auto _vb = (b);                                                        \
        if (!(_va == _vb)) {                                                   \
            std::printf("  [FAIL] %s:%d  %s == %s\n", __FILE__, __LINE__, #a, #b); \
            ++g_failed;                                                        \
        } else {                                                               \
            ++g_passed;                                                        \
        }                                                                      \
    } while (0)

#define RUN_TEST(fn)                                                           \
    do {                                                                       \
        std::printf("[TEST] %s\n", #fn);                                       \
        fn();                                                                  \
    } while (0)

// 每个测试独立的临时目录，避免相互污染
static std::string temp_dir(const std::string& name) {
    static std::atomic<int> counter{0};
    std::string dir = "/tmp/zerokv_test_" + name + "_" + std::to_string(::getpid()) +
                      "_" + std::to_string(counter.fetch_add(1));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

// ---------- 1. DataHandle 位域打包 ----------
static void test_data_handle() {
    // inline 0~5 字节往返
    for (uint32_t len = 0; len <= 5; ++len) {
        char src[8] = {0};
        for (uint32_t i = 0; i < len; ++i) src[i] = static_cast<char>('a' + i);
        DataHandle dh = DataHandle::make_inline(DataType::RAW, src, len);
        CHECK_TRUE(dh.is_inline());
        CHECK_EQ(dh.size(), len);
        char out[8] = {0};
        dh.copy_inline_to(out);
        CHECK_TRUE(std::memcmp(src, out, len) == 0);
    }
    // vlog 字段
    DataHandle vh = DataHandle::make_vlog(DataType::STRING, 7, 12345, 4096);
    CHECK_TRUE(vh.is_vlog());
    CHECK_EQ(vh.vlog_file_id(), 7u);
    CHECK_EQ(vh.vlog_offset(), 12345u);
    CHECK_EQ(vh.size(), 4096u);
    // tombstone
    CHECK_TRUE(DataHandle::make_tombstone().is_tombstone());
    CHECK_TRUE(!DataHandle::make_inline(DataType::RAW, "x", 1).is_tombstone());
}

// ---------- 2. 编解码 ----------
static void test_coding() {
    char buf[8];
    EncodeFixed32(buf, 0xDEADBEEFu);
    CHECK_EQ(DecodeFixed32(buf), 0xDEADBEEFu);
    EncodeFixed64(buf, 0x0123456789ABCDEFULL);
    CHECK_EQ(DecodeFixed64(buf), 0x0123456789ABCDEFULL);
    std::string s;
    PutFixed32(&s, 42u);
    CHECK_EQ(s.size(), 4u);
    CHECK_EQ(DecodeFixed32(s.data()), 42u);
}

// ---------- 3. Bloom Filter ----------
static void test_bloom_filter() {
    const size_t N = 10000;
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; ++i) keys[i] = (static_cast<uint64_t>(i) << 32) | 1ULL;

    auto filter = BloomFilter::BuildFilter(keys);
    CHECK_TRUE(!filter.empty());

    // 已插入的 key 必须全部命中
    for (size_t i = 0; i < N; ++i)
        CHECK_TRUE(BloomFilter::MayMatch(filter.data(), filter.size(), keys[i]));

    // 误报率：使用完全不同的前缀，理论值约 0.8%
    const size_t M = 100000;
    size_t fp = 0;
    for (size_t i = 0; i < M; ++i) {
        uint64_t miss = (static_cast<uint64_t>(N + i) << 32) | 1ULL;
        if (BloomFilter::MayMatch(filter.data(), filter.size(), miss)) ++fp;
    }
    double fpr = static_cast<double>(fp) / M;
    std::printf("  [INFO] Bloom FPR: %.4f (%zu/%zu)\n", fpr, fp, M);
    CHECK_TRUE(fpr < 0.05);
}

// ---------- 4. WAL 往返 ----------
static void test_wal_roundtrip() {
    std::string dir = temp_dir("wal");
    std::string path = dir + "/wal.log";
    const int N = 500;
    {
        WAL wal(path);
        for (int i = 0; i < N; ++i) {
            std::string v = "value_" + std::to_string(i);
            wal.Append(i + 1, static_cast<uint64_t>(i), v.data(),
                       static_cast<uint32_t>(v.size()),
                       (i % 10 == 0) ? WALRecordType::DELETE : WALRecordType::PUT);
        }
        wal.Sync();
    }
    size_t recovered = 0;
    uint64_t max_seq = 0;
    bool bad_value = false;
    uint64_t rs = WAL::Recover(path, [&](uint64_t seq, uint64_t key, const char* v,
                                         uint32_t vlen, WALRecordType) {
        ++recovered;
        max_seq = std::max(max_seq, seq);
        std::string expect = "value_" + std::to_string(static_cast<int>(key));
        if (vlen != expect.size() || std::memcmp(v, expect.data(), vlen) != 0)
            bad_value = true;
    });
    (void)rs;
    CHECK_EQ(recovered, static_cast<size_t>(N));
    CHECK_EQ(max_seq, static_cast<uint64_t>(N));
    CHECK_TRUE(!bad_value);
    std::filesystem::remove_all(dir);
}

// ---------- 5. WAL 尾部撕裂 ----------
static void test_wal_torn_tail() {
    std::string dir = temp_dir("wal_torn");
    std::string path = dir + "/wal.log";
    {
        WAL wal(path);
        std::string v = "payload";
        for (int i = 0; i < 100; ++i)
            wal.Append(i + 1, static_cast<uint64_t>(i), v.data(),
                       static_cast<uint32_t>(v.size()), WALRecordType::PUT);
        wal.Sync();
    }
    // 模拟崩溃时的半写记录：向文件尾追加不完整垃圾字节
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        CHECK_TRUE(fd >= 0);
        if (fd >= 0) {
            char garbage[37];
            std::memset(garbage, 0xAB, sizeof(garbage));
            ssize_t wr = write(fd, garbage, sizeof(garbage));
            (void)wr;
            ::close(fd);
        }
    }
    size_t recovered = 0;
    WAL::Recover(path, [&](uint64_t, uint64_t, const char*, uint32_t, WALRecordType) {
        ++recovered;
    });
    CHECK_EQ(recovered, static_cast<size_t>(100)); // 垃圾尾巴应被正确丢弃
    std::filesystem::remove_all(dir);
}

// ---------- 6. DB 基本读写 + 重开持久化 ----------
static void test_db_basic_and_reopen() {
    std::string dir = temp_dir("db_basic");
    const int N = 5000;
    {
        DB db(dir);
        for (int i = 0; i < N; ++i) {
            std::string v = "val_" + std::to_string(i);
            db.Put(static_cast<uint64_t>(i), v.data(), static_cast<uint32_t>(v.size()));
        }
        std::string v;
        CHECK_TRUE(db.Get(1000, v));
        CHECK_EQ(v, std::string("val_1000"));

        // 覆盖写
        db.Put(1000, "updated", 7);
        CHECK_TRUE(db.Get(1000, v));
        CHECK_EQ(v, std::string("updated"));

        // 删除
        db.Delete(1000);
        CHECK_TRUE(!db.Get(1000, v));
        db.SyncWAL();
    } // 析构 → Flush + 保存 MANIFEST

    {
        DB db(dir); // 重新打开
        std::string v;
        CHECK_TRUE(db.Get(0, v) && v == "val_0");
        CHECK_TRUE(db.Get(N - 1, v) && v == ("val_" + std::to_string(N - 1)));
        CHECK_TRUE(!db.Get(1000, v));   // tombstone 已持久化
        CHECK_TRUE(!db.Get(999999, v)); // 未命中
    }
    std::filesystem::remove_all(dir);
}

// ---------- 7. 范围扫描（重开后，SST 路径）----------
static void test_range_scan_after_reopen() {
    std::string dir = temp_dir("db_scan");
    {
        DB db(dir);
        for (int i = 0; i < 1000; ++i) {
            std::string v = "s" + std::to_string(i);
            db.Put(static_cast<uint64_t>(i * 10), v.data(), static_cast<uint32_t>(v.size()));
        }
        db.Delete(100); // 验证迭代器跳过 tombstone
        db.Delete(500);
        db.SyncWAL();
    }
    {
        DB db(dir);
        auto iter = db.NewIterator();
        iter->Seek(200);
        std::vector<uint64_t> got;
        while (iter->Valid()) {
            got.push_back(iter->key());
            iter->Next();
        }
        CHECK_TRUE(!got.empty());
        CHECK_EQ(got.front(), 200u); // key=200 存在且是 >=200 的第一个
        for (size_t i = 1; i < got.size(); ++i)
            CHECK_TRUE(got[i - 1] < got[i]); // 严格升序
        for (auto k : got)
            CHECK_TRUE(k != 100 && k != 500); // 墓碑被跳过
    }
    std::filesystem::remove_all(dir);
}

// ---------- 7b. 范围扫描（MemTable 未落盘路径：验证迭代器合并 MemTable）----------
static void test_range_scan_memtable() {
    std::string dir = temp_dir("db_scan_mem");
    {
        DB db(dir);
        const int N = 5000;
        for (int i = 0; i < N; ++i) {
            std::string v = "m" + std::to_string(i);
            db.Put(static_cast<uint64_t>(i * 2), v.data(), static_cast<uint32_t>(v.size()));
        }
        // 全部数据仍在 MemTable（未触发 16MB flush），直接扫描
        auto iter = db.NewIterator();
        iter->Seek(100);
        std::vector<uint64_t> got;
        while (iter->Valid() && got.size() < N) {
            got.push_back(iter->key());
            iter->Next();
        }
        CHECK_TRUE(!got.empty());
        CHECK_EQ(got.front(), 100u);                    // key=100 (i=50) 是 >=100 的第一个
        CHECK_TRUE(got.size() >= static_cast<size_t>(N - 50));
        for (size_t i = 1; i < got.size(); ++i)
            CHECK_TRUE(got[i - 1] < got[i]);

        // 删除一个 key 后立即扫描：墓碑在 MemTable 层也应被跳过
        db.Delete(200);
        auto iter2 = db.NewIterator();
        iter2->Seek(100);
        while (iter2->Valid()) {
            CHECK_TRUE(iter2->key() != 200);
            iter2->Next();
        }
    }
    std::filesystem::remove_all(dir);
}

// ---------- 8. VLog GC 完整性 ----------
static void test_db_gc() {
    std::string dir = temp_dir("db_gc");
    const int N = 2000;
    {
        DB db(dir);
        std::string v1(512, 'x'), v2(512, 'y'), v3(512, 'z');
        for (int i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v1.data(), 512);
        for (int i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v2.data(), 512);
        for (int i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v3.data(), 512);
        db.SyncWAL();
        db.WaitForBackgroundJobs();

        int rounds = db.RunFullGC();
        std::printf("  [INFO] GC rounds: %d\n", rounds);

        std::string v;
        for (int i = 0; i < N; ++i) {
            CHECK_TRUE(db.Get(static_cast<uint64_t>(i), v));
            if (v != v3) {
                std::printf("  [FAIL] GC integrity broken at key %d\n", i);
                ++g_failed;
                break;
            }
        }
    }
    std::filesystem::remove_all(dir);
}

// ---------- 9. 多线程并发写 ----------
struct ThreadLocalGuard {
    using NodePool = TlsMemPoolManager<LockFreeSkipList<uint64_t>::NodeType>;
    ThreadLocalGuard() { NodePool::instance().InitThreadLocalPool(); }
    ~ThreadLocalGuard() {
        EpochManager::instance().try_reclaim();
        NodePool::instance().ReleaseThreadLocalPool();
    }
};

static void test_concurrent_puts() {
    std::string dir = temp_dir("db_concurrent");
    const int THREADS = 4;
    const int PER = 5000;
    {
        DB db(dir);
        std::vector<std::thread> ths;
        for (int t = 0; t < THREADS; ++t) {
            ths.emplace_back([&db, t]() {
                ThreadLocalGuard guard;
                for (int i = 0; i < PER; ++i) {
                    uint64_t key = (static_cast<uint64_t>(t) << 48) | static_cast<uint64_t>(i);
                    std::string v = "t" + std::to_string(t) + "_value_" + std::to_string(i);
                    db.Put(key, v.data(), static_cast<uint32_t>(v.size()));
                }
            });
        }
        for (auto& th : ths) th.join();
        db.SyncWAL();
        db.WaitForBackgroundJobs();

        for (int t = 0; t < THREADS; ++t) {
            for (int i = 0; i < PER; ++i) {
                uint64_t key = (static_cast<uint64_t>(t) << 48) | static_cast<uint64_t>(i);
                std::string expect = "t" + std::to_string(t) + "_value_" + std::to_string(i);
                std::string v;
                CHECK_TRUE(db.Get(key, v));
                if (v != expect) {
                    std::printf("  [FAIL] concurrent value mismatch t=%d i=%d\n", t, i);
                    ++g_failed;
                    break;
                }
            }
        }
    }
    std::filesystem::remove_all(dir);
}

// ---------- 9b. 同一 Key 并发覆盖（追加式版本链：无撕裂 + 版本链记账）----------
static void test_concurrent_same_key() {
    std::string dir = temp_dir("db_same_key");
    {
        DB db(dir);
        // 顺序覆盖：Get 必须始终返回最后一次写入
        for (int i = 0; i < 1000; ++i) {
            std::string v = "seq_" + std::to_string(i);
            db.Put(42, v.data(), static_cast<uint32_t>(v.size()));
        }
        std::string v;
        CHECK_TRUE(db.Get(42, v));
        CHECK_EQ(v, std::string("seq_999"));

        // 多线程并发覆盖同一个 key：最终值必须是某次写入的完整值（无撕裂）
        const int THREADS = 4, ROUNDS = 2000;
        std::vector<std::string> finals(THREADS);
        std::vector<std::thread> ths;
        for (int t = 0; t < THREADS; ++t) {
            finals[t] = std::string("t") + std::to_string(t) + "_" + std::string(64, static_cast<char>('a' + t));
            ths.emplace_back([&db, t, &finals, ROUNDS]() {
                ThreadLocalGuard guard;
                for (int i = 0; i < ROUNDS; ++i)
                    db.Put(7, finals[t].data(), static_cast<uint32_t>(finals[t].size()));
            });
        }
        for (auto& th : ths) th.join();
        std::string got;
        CHECK_TRUE(db.Get(7, got));
        bool matches = false;
        for (auto& f : finals)
            if (got == f) { matches = true; break; }
        CHECK_TRUE(matches);

        // Delete + GC 后语义仍正确（GC 完整性）
        db.Delete(42);
        db.SyncWAL();
        db.WaitForBackgroundJobs();
        int rounds = db.RunFullGC();
        std::printf("  [INFO] same-key GC rounds: %d\n", rounds);
        CHECK_TRUE(!db.Get(42, got));
        CHECK_TRUE(db.Get(7, got));
    }
    std::filesystem::remove_all(dir);
}

// ---------- 9c. 删除语义跨 Compaction 保持（墓碑去重回归测试）----------
// 修复前：CompactionTask 的墓碑去重顺序有 bug，最新版本为墓碑时 prev_key
// 未更新，旧版本会被写回底层，导致"已删除的 key 在 compaction 后复活"。
static void test_delete_survives_compaction() {
    std::string dir = temp_dir("db_resurrect");
    {
        DB db(dir);
        std::string v = "old_value_for_delete";
        db.Put(99, v.data(), static_cast<uint32_t>(v.size()));
        db.SyncWAL();
    } // 关闭 → Flush 到 SST
    {
        DB db(dir);
        db.Delete(99);
        db.ForceGlobalCompaction(); // 全局压缩：合并 L0/L1，底层丢墓碑
        std::string v;
        CHECK_TRUE(!db.Get(99, v)); // ✅ 修复前：被删 key 会在 compaction 后"复活"
        db.RunFullGC();
        CHECK_TRUE(!db.Get(99, v)); // GC 后依然保持删除语义
    }
    std::filesystem::remove_all(dir);
}

int main() {
    RUN_TEST(test_data_handle);
    RUN_TEST(test_coding);
    RUN_TEST(test_bloom_filter);
    RUN_TEST(test_wal_roundtrip);
    RUN_TEST(test_wal_torn_tail);
    RUN_TEST(test_db_basic_and_reopen);
    RUN_TEST(test_range_scan_after_reopen);
    RUN_TEST(test_range_scan_memtable);
    RUN_TEST(test_db_gc);
    RUN_TEST(test_concurrent_puts);
    RUN_TEST(test_concurrent_same_key);
    RUN_TEST(test_delete_survives_compaction);

    std::printf("\n========================================\n");
    std::printf("PASSED: %d   FAILED: %d\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}