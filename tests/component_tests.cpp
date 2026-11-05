// ============================================================
// ZeroKV 组件级测试：逐组件验证正确性 + 微基准
// 编译：g++ -O2 -std=c++20 -march=native component_tests.cpp -o component_tests -llz4 -lz -lpthread
// 运行：./component_tests
// ============================================================
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "DataHandle.h"
#include "Coding.hpp"
#include "BloomFilter.hpp"
#include "WAL.hpp"
#include "SSTable.hpp"
#include "PinnableSlice.hpp"
#include "ValueLog.hpp"
#include "LockFreeSkipList.hpp"
#include "EpochManager.hpp"
#include "MemPool.hpp"
#include "DB.hpp"

using namespace kvstore;

class Timer {
    std::chrono::high_resolution_clock::time_point start_;
public:
    Timer() { start_ = std::chrono::high_resolution_clock::now(); }
    double elapsed_ms() const {
        return std::chrono::duration<double, std::milli>(
            std::chrono::high_resolution_clock::now() - start_).count();
    }
};

static std::string temp_dir(const std::string& name) {
    static std::atomic<int> counter{0};
    std::string dir = "/tmp/zerokv_ct_" + name + "_" + std::to_string(::getpid()) +
                      "_" + std::to_string(counter.fetch_add(1));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static size_t dir_size(const std::string& dir) {
    size_t total = 0;
    if (!std::filesystem::exists(dir)) return 0;
    try {
        for (auto& e : std::filesystem::recursive_directory_iterator(dir))
            if (e.is_regular_file()) total += e.file_size();
    } catch (...) {}
    return total;
}

struct ThreadLocalGuard {
    using NodePool = TlsMemPoolManager<LockFreeSkipList<uint64_t>::NodeType>;
    ThreadLocalGuard() { NodePool::instance().InitThreadLocalPool(); }
    ~ThreadLocalGuard() {
        EpochManager::instance().try_reclaim();
        NodePool::instance().ReleaseThreadLocalPool();
    }
};

// ============ 1. DataHandle ============
static bool test_data_handle() {
    bool ok = true;
    for (uint32_t len = 0; len <= 5; ++len) {
        char src[8] = {0};
        for (uint32_t i = 0; i < len; ++i) src[i] = static_cast<char>('a' + i);
        DataHandle dh = DataHandle::make_inline(DataType::RAW, src, len);
        if (!dh.is_inline() || dh.size() != len) { ok = false; std::printf("  [FAIL] inline len=%u\n", len); }
        char out[8] = {0};
        dh.copy_inline_to(out);
        if (std::memcmp(src, out, len) != 0) { ok = false; std::printf("  [FAIL] inline roundtrip len=%u\n", len); }
    }
    DataHandle vh = DataHandle::make_vlog(DataType::STRING, 7, 12345, 4096);
    if (!vh.is_vlog() || vh.vlog_file_id() != 7 || vh.vlog_offset() != 12345 || vh.size() != 4096) ok = false;
    if (!DataHandle::make_tombstone().is_tombstone()) ok = false;

    const size_t M = 2000000;
    char src[5] = {'h', 'e', 'l', 'l', 'o'};
    Timer t;
    for (size_t i = 0; i < M; ++i) {
        DataHandle dh = DataHandle::make_inline(DataType::RAW, src, 5);
        char out[8] = {0};
        dh.copy_inline_to(out);
    }
    double us = t.elapsed_ms() *1000.00 ;
    std::printf("  [1/9] DataHandle    %s | 2,000,000 encode/decode: %.2f us \n",
                ok ? "✅ PASS" : "❌ FAIL", us);
    return ok;
}

// ============ 2. Coding ============
static bool test_coding() {
    bool ok = true;
    char buf[8];
    EncodeFixed32(buf, 0xDEADBEEFu);
    if (DecodeFixed32(buf) != 0xDEADBEEFu) ok = false;
    EncodeFixed64(buf, 0x0123456789ABCDEFULL);
    if (DecodeFixed64(buf) != 0x0123456789ABCDEFULL) ok = false;
    std::string s;
    PutFixed32(&s, 42u);
    if (s.size() != 4 || DecodeFixed32(s.data()) != 42u) ok = false;

    const size_t M = 2000000;
    Timer t;
    for (size_t i = 0; i < M; ++i) {
        EncodeFixed64(buf, i);
        if (DecodeFixed64(buf) != i) { ok = false; break; }
    }
    double us = t.elapsed_ms() * 1000.00;
    std::printf("  [2/9] Coding        %s | 2,000,000 encode/decode: %.2f us\n",
                ok ? "✅ PASS" : "❌ FAIL", us);
    return ok;
}

// ============ 3. BloomFilter ============
static bool test_bloom_filter() {
    bool ok = true;
    const size_t N = 10000, M = 100000;
    std::vector<uint64_t> keys(N);
    for (size_t i = 0; i < N; ++i) keys[i] = (static_cast<uint64_t>(i) << 32) | 1ULL;

    Timer t;
    auto filter = BloomFilter::BuildFilter(keys);
    double build_ms = t.elapsed_ms();

    Timer t2;
    for (size_t i = 0; i < N; ++i)
        if (!BloomFilter::MayMatch(filter.data(), filter.size(), keys[i])) ok = false;
    size_t fp = 0;
    for (size_t i = 0; i < M; ++i) {
        uint64_t miss = (static_cast<uint64_t>(N + i) << 32) | 1ULL;
        if (BloomFilter::MayMatch(filter.data(), filter.size(), miss)) ++fp;
    }
    double query_ms = t2.elapsed_ms();
    double fpr = static_cast<double>(fp) / M;
    if (fpr > 0.05) ok = false;
    std::printf("  [3/9] BloomFilter   %s | build %zu keys: %.2f ms | query %zu: %.2f ms | FPR=%.3f%%\n",
                ok ? "✅ PASS" : "❌ FAIL", N, build_ms, N + M, query_ms, fpr * 100.0);
    return ok;
}

// ============ 4. WAL ============
static bool test_wal() {
    bool ok = true;
    std::string dir = temp_dir("wal");
    std::string path = dir + "/wal.log";
    const int N = 100000;
    Timer t;
    {
        WAL wal(path);
        for (int i = 0; i < N; ++i) {
            std::string v = "wal_value_" + std::to_string(i);
            wal.Append(i + 1, static_cast<uint64_t>(i), v.data(),
                       static_cast<uint32_t>(v.size()), WALRecordType::PUT);
        }
        wal.Sync();
    }
    double append_ms = t.elapsed_ms();

    size_t recovered = 0;
    bool bad = false;
    Timer t2;
    WAL::Recover(path, [&](uint64_t, uint64_t key, const char* v, uint32_t vlen, WALRecordType) {
        ++recovered;
        std::string expect = "wal_value_" + std::to_string(static_cast<int>(key));
        if (vlen != expect.size() || std::memcmp(v, expect.data(), vlen) != 0) bad = true;
    });
    double recover_ms = t2.elapsed_ms();
    if (recovered != static_cast<size_t>(N) || bad) ok = false;

    // 尾部撕裂：追加半写垃圾，恢复应只回放完整记录
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        if (fd >= 0) {
            char garbage[41];
            std::memset(garbage, 0xAB, sizeof(garbage));
            ssize_t w = ::write(fd, garbage, sizeof(garbage));
            (void)w;
            ::close(fd);
        }
        size_t c2 = 0;
        WAL::Recover(path, [&](uint64_t, uint64_t, const char*, uint32_t, WALRecordType) { ++c2; });
        if (c2 != static_cast<size_t>(N)) { ok = false; std::printf("  [FAIL] torn-tail recovery: %zu/%d\n", c2, N); }
    }
    std::printf("  [4/9] WAL           %s | append %d: %.2f ms | recover: %.2f ms | torn-tail OK\n",
                ok ? "✅ PASS" : "❌ FAIL", N, append_ms, recover_ms);
    std::filesystem::remove_all(dir);
    return ok;
}

// ============ 5. LockFreeSkipList ============
static bool test_skiplist() {
    bool ok = true;
    const size_t N = 200000;
    LockFreeSkipList<uint64_t> sl;

    Timer t;
    for (uint64_t i = 0; i < N; ++i) {
        DataHandle dh(i);
        sl.Put(i, dh, i + 1);
    }
    double put_ms = t.elapsed_ms();

    Timer t2;
    for (uint64_t i = 0; i < N; ++i) {
        uint64_t raw = 0, seq = 0;
        if (!sl.GetHandle(i, raw, seq) || raw != i || seq != i + 1) { ok = false; break; }
    }
    uint64_t raw = 0, seq = 0;
    if (sl.GetHandle(99999999ULL, raw, seq)) ok = false; // miss 应返回 false
    double get_ms = t2.elapsed_ms();

    // 4 线程并发写不同 key
    const int T = 4;
    const size_t PER = 100000;
    std::atomic<bool> conc_ok{true};
    std::vector<std::thread> ths;
    Timer t3;
    for (int tid = 0; tid < T; ++tid) {
        ths.emplace_back([&sl, tid, PER]() {
            ThreadLocalGuard guard;
            for (size_t i = 0; i < PER; ++i) {
                uint64_t k = (static_cast<uint64_t>(tid) << 48) | i;
                DataHandle dh(k);
                sl.Put(k, dh, k + 1);
            }
        });
    }
    for (auto& th : ths) th.join();
    for (int tid = 0; tid < T; ++tid) {
        for (size_t i = 0; i < PER; ++i) {
            uint64_t k = (static_cast<uint64_t>(tid) << 48) | i, r = 0, s = 0;
            if (!sl.GetHandle(k, r, s) || r != k) conc_ok = false;
        }
    }
    double conc_ms = t3.elapsed_ms();
    if (!conc_ok) ok = false;
    std::printf("  [5/9] SkipList      %s | put %zu: %.2f ms | get %zu: %.2f ms | 4T x %zu: %.2f ms\n",
                ok ? "✅ PASS" : "❌ FAIL", N, put_ms, N, get_ms, PER, conc_ms);
    return ok;
}

// ============ 6. SSTable ============
static bool test_sstable() {
    bool ok = true;
    std::string dir = temp_dir("sst");
    std::string path = dir + "/test.sst";
    const size_t N = 100000;
    Timer t;
    {
        SSTWriter<uint64_t> w(path);
        for (uint64_t i = 0; i < N; ++i) {
            DataHandle dh = DataHandle::make_vlog(DataType::RAW, 1, static_cast<uint32_t>(i), 64);
            w.Add(i, dh, i + 1);
        }
        w.Finish();
    }
    double write_ms = t.elapsed_ms();
    size_t file_size = std::filesystem::file_size(path);

    SSTReader<uint64_t> r(path, 1);
    Timer t2;
    size_t found = 0;
    for (uint64_t i = 0; i < N; ++i) {
        PinnableSlice v;
        uint64_t s = 0;
        if (r.Get(i, v, s) && v.size() == 8) {
            uint64_t rawv = 0;
            std::memcpy(&rawv, v.data(), 8);
            if (DataHandle(rawv).is_vlog() && DataHandle(rawv).vlog_offset() == i) ++found;
        }
    }
    double get_ms = t2.elapsed_ms();
    if (found != N) ok = false;

    // 范围扫描（迭代器，从 N/2 扫到末尾）
    Timer t3;
    size_t scanned = 0;
    bool ordered = true;
    uint64_t prev = 0;
    auto it = r.NewIterator();
    it->Seek(N / 2);
    while (it->Valid()) {
        if (scanned > 0 && it->key() <= prev) ordered = false;
        prev = it->key();
        ++scanned;
        it->Next();
    }
    double scan_ms = t3.elapsed_ms();
    if (!ordered || scanned != N - N / 2) { ok = false; std::printf("  [FAIL] scan %zu entries\n", scanned); }
    std::printf("  [6/9] SSTable       %s | write %zu: %.2f ms (%zu KB) | point get: %.2f ms | scan: %.2f ms (%zu entries)\n",
                ok ? "✅ PASS" : "❌ FAIL", N, write_ms, file_size / 1024, get_ms, scan_ms, scanned);
    std::filesystem::remove_all(dir);
    return ok;
}

// ============ 7. ValueLog ============
static bool test_valuelog() {
    bool ok = true;
    std::string dir = temp_dir("vlog");
    const size_t N = 100000;
    std::vector<uint32_t> fids(N), offs(N);
    std::vector<std::string> vals(N);
    {
        ValueLog vlog(dir);
        vlog.RecoverAndTruncate();
        Timer t;
        for (size_t i = 0; i < N; ++i) {
            vals[i] = "vlog_value_" + std::to_string(i);
            vlog.Append(static_cast<uint64_t>(i), vals[i].data(),
                        static_cast<uint32_t>(vals[i].size()), fids[i], offs[i]);
        }
        vlog.Sync();
        double append_ms = t.elapsed_ms();

        Timer t2;
        size_t good = 0;
        for (size_t i = 0; i < N; ++i) {
            PinnableSlice s;
            if (vlog.Read(fids[i], offs[i], static_cast<uint32_t>(vals[i].size()), s) &&
                s.size() == vals[i].size() &&
                std::memcmp(s.data(), vals[i].data(), vals[i].size()) == 0)
                ++good;
        }
        double read_ms = t2.elapsed_ms();
        if (good != N) ok = false;
        std::printf("  [7/9] ValueLog      %s | append %zu: %.2f ms | read back: %.2f ms | %zu/%zu intact\n",
                    ok ? "✅ PASS" : "❌ FAIL", N, append_ms, read_ms, good, N);
    }
    std::filesystem::remove_all(dir);
    return ok;
}

// ============ 8. DB + VLog GC ============
static bool test_db_gc() {
    bool ok = true;
    std::string dir = temp_dir("db_gc");
    const size_t N = 20000;
    {
        DB db(dir);
        std::string v1(256, 'x'), v2(256, 'y'), v3(256, 'z');
        Timer t;
        for (size_t i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v1.data(), static_cast<uint32_t>(v1.size()));
        for (size_t i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v2.data(), static_cast<uint32_t>(v2.size()));
        for (size_t i = 0; i < N; ++i)
            db.Put(static_cast<uint64_t>(i), v3.data(), static_cast<uint32_t>(v3.size()));
        for (size_t i = 0; i < 1000; ++i)
            db.Delete(static_cast<uint64_t>(i));
        db.SyncWAL();
        db.WaitForBackgroundJobs();
        size_t before = dir_size(dir);
        int rounds = db.RunFullGC();
        db.WaitForBackgroundJobs();
        size_t after = dir_size(dir);
        double ms = t.elapsed_ms();

        size_t correct = 0;
        std::string v;
        for (size_t i = 0; i < N; ++i) {
            if (db.Get(static_cast<uint64_t>(i), v) && v == v3) ++correct;
        }
        bool del_ok = true;
        for (size_t i = 0; i < 1000; ++i) {
            std::string tmp;
            if (db.Get(static_cast<uint64_t>(i), tmp)) del_ok = false;
        }
        if (correct != N - 1000 || !del_ok || after >= before) {
            ok = false;
            std::printf("  [FAIL] gc integrity %zu/%zu\n", correct, N - 1000);
        }
        size_t reclaimed = (after < before) ? (before - after) / 1024 : 0;
        std::printf("  [8/9] DB + VLog GC %s | %zu keys x 3 versions + 1000 deletes | rounds: %d | reclaimed: %zu KB | integrity: %zu/%zu | total: %.2f ms\n",
                    ok ? "✅ PASS" : "❌ FAIL", N, rounds, reclaimed, correct, N - 1000, ms);
    }
    std::filesystem::remove_all(dir);
    return ok;
}

// ============ 9. DBIterator ============
static bool test_dbiterator() {
    bool ok = true;
    std::string dir = temp_dir("db_iter");
    const size_t N = 5000;
    {
        DB db(dir);
        // 全部数据仍在 MemTable（未触发 16MB flush）
        for (size_t i = 0; i < N; ++i) {
            std::string v = "it_" + std::to_string(i);
            db.Put(static_cast<uint64_t>(i * 2), v.data(), static_cast<uint32_t>(v.size()));
        }
        db.Delete(100); // 墓碑应在扫描中被跳过

        Timer t;
        auto it = db.NewIterator();
        it->Seek(100);
        size_t seen = 0;
        bool ordered = true;
        uint64_t prev = 0;
        while (it->Valid()) {
            if (seen > 0 && it->key() <= prev) ordered = false;
            prev = it->key();
            ++seen;
            it->Next();
        }
        double ms = t.elapsed_ms();
        const size_t expect = N - 51; // 100 之前 50 个 key + 被删除的 100 本身
        if (!ordered || seen != expect) {
            ok = false;
            std::printf("  [FAIL] memtable scan %zu entries (expect %zu)\n", seen, expect);
        }
        std::printf("  [9/9] DBIterator    %s | memtable scan (unflushed): %zu/%zu | %.2f ms | %s\n",
                    ok ? "✅ PASS" : "❌ FAIL", seen, expect, ms, ordered ? "✅ ordered" : "❌ MISMATCH");
    }
    std::filesystem::remove_all(dir);
    return ok;
}

int main() {
    std::printf("==========================================================\n");
    std::printf(" ZeroKV Component Tests\n");
    std::printf("==========================================================\n");
    bool all = true;
    all &= test_data_handle();
    all &= test_coding();
    all &= test_bloom_filter();
    all &= test_wal();
    all &= test_skiplist();
    all &= test_sstable();
    all &= test_valuelog();
    all &= test_db_gc();
    all &= test_dbiterator();
    std::printf("----------------------------------------------------------\n");
    std::printf(" %s All component tests passed.\n", all ? "🎉" : "❌");
    return all ? 0 : 1;
}