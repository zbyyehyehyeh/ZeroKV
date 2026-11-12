#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <cstdint>
#include <algorithm>
#include <thread>
#include <atomic>
#include <cassert>
#include <filesystem>
#include <sys/stat.h>
#include <cstdlib>
#include <unistd.h> // for geteuid

#include "DB.hpp"
#include "EpochManager.hpp" 

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

void print_sep() { 
    std::cout << "--------------------------------------------------------------\n"; 
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

void safe_remove_all(const std::string& path) {
    for (int i = 0; i < 5; ++i) {
        try {
            if (std::filesystem::exists(path))
                std::filesystem::remove_all(path);
            return;
        } catch (...) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

bool is_root() {
    return geteuid() == 0;
}

void DropPageCache() {
    if (!is_root()) {
        std::cout << "  [Info] Not running as root. Skipping OS page cache drop.\n";
        std::cout << "         For accurate cold-read IO, run: sudo ./bench\n";
        return;
    }
    std::cout << "  [Info] Dropping OS Page Cache...\n";
    if (system("sync") != 0) {
        std::cout << "  [Warning] 'sync' failed.\n";
    }
    int ret = system("echo 3 > /proc/sys/vm/drop_caches 2>/dev/null");
    if (ret != 0) {
        std::cout << "  [Warning] Failed to drop caches despite root privileges.\n";
    } else {
        std::cout << "  [Info] Page Cache dropped successfully.\n";
    }
}

// RAII guard for thread-local memory pool
struct ThreadLocalGuard {
    using NodePool = TlsMemPoolManager<typename LockFreeSkipList<uint64_t>::NodeType>;
    ThreadLocalGuard() { NodePool::instance().InitThreadLocalPool(); }
    ~ThreadLocalGuard() { 
        EpochManager::instance().try_reclaim();
        NodePool::instance().ReleaseThreadLocalPool(); 
    }
};

// zero-copy read: touch 1 byte to force page-in / avoid DCE, no full copy
static inline bool zget(kvstore::DB& db, uint64_t k) {
    PinnableSlice s;
    if (!db.GetZeroCopy(k, s)) return false;
    if (s.size() > 0) { volatile char c = s.data()[0]; (void)c; }
    return true;
}

int main() {
    const std::string TEST_DIR = "/tmp/zerokv_bench";
    safe_remove_all(TEST_DIR);
    std::filesystem::create_directories(TEST_DIR);

    const size_t N = 500000;       
    const size_t HIT_Q = 200000;   
    const size_t MISS_Q = 200000;  
    const size_t RANGE_Q = 10000;  
    const int NUM_THREADS = 4;

    std::mt19937_64 rng(42);
    std::cout << "🚀 ZeroKV Full Benchmark | N=" << N 
              << " | Threads=" << NUM_THREADS << "\n";
    print_sep();

    std::vector<uint64_t> written_keys;
    written_keys.reserve(N);

    try {
        // ========== 1. 写入性能 ==========
        std::cout << "[1/7] Sequential Write Performance\n";
        {
            DB db(TEST_DIR);
            Timer t;
            for (size_t i = 0; i < N; ++i) {
                uint64_t key = (rng() % 100000ULL) << 32 | (i & 0xFFFFFFFF);
                written_keys.push_back(key);
                
                char val[32];
                snprintf(val, sizeof(val), "value_%zu", i);
                db.Put(key, val, strlen(val));
            }
            db.SyncWAL();
            db.WaitForBackgroundJobs();
            double ms = t.elapsed_ms();
            std::cout << "  ✅ Wrote " << N << " keys: " << std::fixed << std::setprecision(2) 
                      << ms << " ms | " << std::setprecision(0) << (N / (ms/1000.0)) << " ops/s\n";
            std::cout << "  📦 Disk usage: " << dir_size(TEST_DIR) / 1024 << " KB\n";
        }
        print_sep();

        // ========== 2. Point Get Hit ==========
        std::cout << "[2/7] Point Get (Hit)\n";
        {
            DB db(TEST_DIR);
            // Warmup
            for (size_t i = 0; i < 1000; ++i) {
                uint64_t key = written_keys[rng() % N]; 
                std::string v; db.Get(key, v);
            }
            
            std::vector<uint64_t> keys(HIT_Q);
            for (size_t i = 0; i < HIT_Q; ++i)
                keys[i] = written_keys[rng() % N]; 

            Timer t;
            size_t found = 0;
            for (auto k : keys) { 
                if (zget(db, k)) found++; 
            }
            double ms = t.elapsed_ms();
            std::cout << "  ✅ Hit: " << found << "/" << HIT_Q << " | " 
                      << std::fixed << std::setprecision(2) << ms << " ms | "
                      << std::setprecision(0) << (HIT_Q / (ms/1000.0)) << " QPS\n";
        }
        print_sep();

        // ========== 3. Point Get Miss (Hot vs Cold) ==========
        std::cout << "[3/7] Point Get (Miss) - Hot vs Cold\n";
        {
            // Hot Miss
            DB db_hot(TEST_DIR);
            std::vector<uint64_t> hot_keys(MISS_Q);
            for (size_t i = 0; i < MISS_Q; ++i)
                hot_keys[i] = (200000ULL + i) << 32 | 1; 

            Timer t_hot;
            size_t found_hot = 0;
            for (auto k : hot_keys) { 
                if (zget(db_hot, k)) found_hot++; 
            }
            double ms_hot = t_hot.elapsed_ms();
            std::cout << "  🔥 Hot Miss: " << std::setprecision(2) << ms_hot << " ms | "
                      << std::setprecision(0) << (MISS_Q / (ms_hot/1000.0)) << " QPS\n";
            
            // Cold Miss
            DropPageCache();
            DB db_cold(TEST_DIR); // Fresh instance to clear block cache
            
            std::vector<uint64_t> cold_keys(MISS_Q);
            for (size_t i = 0; i < MISS_Q; ++i)
                cold_keys[i] = (300000ULL + i) << 32 | 1;

            Timer t_cold;
            size_t found_cold = 0;
            for (auto k : cold_keys) { 
                if (zget(db_cold, k)) found_cold++; 
            }
            double ms_cold = t_cold.elapsed_ms();
            std::cout << "  ❄️  Cold Miss: " << std::setprecision(2) << ms_cold << " ms | "
                      << std::setprecision(0) << (MISS_Q / (ms_cold/1000.0)) << " QPS\n";
            std::cout << "  ❌ False Positives: " << (found_hot + found_cold) << "\n";
        }
        print_sep();

        // ========== 4. Range Scan (SST + MemTable) ==========
        std::cout << "[4/7] Range Scan (Seek + Next x100, SST + MemTable)\n";
        {
            DB db(TEST_DIR);
            // ---- 4a. SST 路径（复用前文已落盘的数据）----
            Timer t;
            size_t total_entries = 0;
            std::uniform_int_distribution<size_t> dist(0, N - 100);
            
            for (size_t i = 0; i < RANGE_Q; ++i) {
                auto iter = db.NewIterator();
                uint64_t start_key = written_keys[dist(rng)];
                iter->Seek(start_key);
                int count = 0;
                while (iter->Valid() && count < 100) {
                    std::string v; 
                    iter->GetValue(v);
                    iter->Next(); 
                    count++;
                }
                total_entries += count;
            }
            double ms = t.elapsed_ms();
            std::cout << "  ✅ SST scans: " << RANGE_Q
                      << " | " << std::fixed << std::setprecision(2) << ms << " ms | "
                      << total_entries << " entries read\n";

            // ---- 4b. MemTable 路径（新写入未落盘，验证迭代器已合并 MemTable）----
            const size_t MEM_Q = 50000;
            std::vector<uint64_t> mem_keys;
            mem_keys.reserve(MEM_Q);
            for (size_t i = 0; i < MEM_Q; ++i) {
                uint64_t key = ((400000ULL + i) << 32) | 7;
                mem_keys.push_back(key);
                char val[32];
                snprintf(val, sizeof(val), "mem_%zu", i);
                db.Put(key, val, strlen(val));
            }
            Timer t2;
            auto iter = db.NewIterator();
            iter->Seek(mem_keys.front());
            size_t seen = 0;
            bool all_ok = true;
            while (iter->Valid() && seen < MEM_Q) {
                if (iter->key() != mem_keys[seen]) { all_ok = false; break; }
                std::string v;
                iter->GetValue(v);
                iter->Next();
                seen++;
            }
            double ms2 = t2.elapsed_ms();
            std::cout << "  ✅ MemTable scans (unflushed): " << seen << "/" << MEM_Q
                      << " visible | " << std::fixed << std::setprecision(2) << ms2 << " ms | "
                      << (all_ok ? "✅ ordered" : "❌ MISMATCH") << "\n";
            assert(all_ok && seen == MEM_Q);
        }
        print_sep();

        // ========== 5. Concurrent Write Safety ==========
        std::cout << "[5/7] Concurrent Write Safety (" << NUM_THREADS << " threads)\n";
        {
            std::string cdir = TEST_DIR + "/concurrent";
            std::filesystem::create_directories(cdir);
            DB db(cdir);
            std::atomic<size_t> written{0};
            std::vector<std::thread> threads;
            
            Timer t;
            for (int tid = 0; tid < NUM_THREADS; ++tid) {
                threads.emplace_back([&, tid]() {
                    ThreadLocalGuard guard; // RAII ensures cleanup
                    for (size_t i = 0; i < N / NUM_THREADS; ++i) {
                        uint64_t key = (static_cast<uint64_t>(tid) << 48) | i;
                        char val[16]; 
                        snprintf(val, sizeof(val), "t%d_%zu", tid, i);
                        db.Put(key, val, strlen(val));
                        written.fetch_add(1, std::memory_order_relaxed);
                    }
                });
            }
            for (auto& th : threads) th.join();
            
            db.SyncWAL();
            db.WaitForBackgroundJobs();
            double ms = t.elapsed_ms();
            
            size_t verified = 0;
            for (int tid = 0; tid < NUM_THREADS; ++tid) {
                for (size_t i = 0; i < N / NUM_THREADS; ++i) {
                    uint64_t key = (static_cast<uint64_t>(tid) << 48) | i;
                    std::string v;
                    if (db.Get(key, v)) verified++;
                }
            }
            std::cout << "  ✅ Written: " << written.load() << " | Verified: " << verified 
                      << " | " << std::fixed << std::setprecision(2) << ms << " ms | "
                      << std::setprecision(0) << (written.load() / (ms/1000.0)) << " ops/s\n";
            assert(verified == written.load());
        } 
        safe_remove_all(TEST_DIR + "/concurrent");
        print_sep();

        // ========== 6. Crash Recovery ==========
        std::cout << "[6/7] WAL Crash Recovery\n";
        {
            std::string rdir = TEST_DIR + "/recovery";
            std::filesystem::create_directories(rdir);
            constexpr size_t REC_N = 10000;
            {
                DB db(rdir);
                for (size_t i = 0; i < REC_N; ++i) {
                    uint64_t key = i << 32 | 1;
                    char val[16]; 
                    snprintf(val, sizeof(val), "rec_%zu", i);
                    db.Put(key, val, strlen(val));
                }
                db.SyncWAL();
            }
            // Reopen to simulate recovery
            DB db(rdir);
            size_t recovered = 0;
            for (size_t i = 0; i < REC_N; ++i) {
                uint64_t key = i << 32 | 1;
                std::string v;
                if (db.Get(key, v)) recovered++;
            }
            std::cout << "  ✅ Before crash: " << REC_N 
                      << " | After recovery: " << recovered << "\n";
            assert(recovered == REC_N);
        }
        safe_remove_all(TEST_DIR + "/recovery");
        print_sep();

        // ========== 7. VLog GC ==========
        std::cout << "[7/7] VLog Garbage Collection\n";
        {
            std::string gdir = TEST_DIR + "/gc_test";
            std::filesystem::create_directories(gdir);
            DB db(gdir);
            
            const size_t GC_N = 100000;
            std::string val_init(1024, 'x'); 
            
            for (size_t i = 0; i < GC_N; ++i) {
                uint64_t key = i << 32 | 1;
                db.Put(key, val_init.data(), val_init.size());
            }
            db.SyncWAL();
            db.WaitForBackgroundJobs();
            size_t size_before = dir_size(gdir);
            
            // Overwrite twice to create garbage
            std::string val_update1(1024, 'y');
            for (size_t i = 0; i < GC_N; ++i) {
                uint64_t key = i << 32 | 1;
                db.Put(key, val_update1.data(), val_update1.size());
            }
            
            std::string val_update2(1024, 'z');
            for (size_t i = 0; i < GC_N; ++i) {
                uint64_t key = i << 32 | 1;
                db.Put(key, val_update2.data(), val_update2.size());
            }
            db.SyncWAL();
            db.WaitForBackgroundJobs();
            
            size_t size_before_gc = dir_size(gdir);
            
            int gc_rounds = db.RunFullGC();
            db.WaitForBackgroundJobs();
            size_t size_after = dir_size(gdir);
            
            // Verify only latest version exists
            size_t correct = 0;
            for (size_t i = 0; i < GC_N; ++i) {
                uint64_t key = i << 32 | 1;
                std::string v;
                if (db.Get(key, v) && v.size() == 1024 && v[0] == 'z') 
                    correct++;
            }
            
            std::cout << "  ✅ GC Rounds: " << gc_rounds << "\n";
            std::cout << "  📦 Size (init):      " << size_before / 1024 << " KB\n";
            std::cout << "  📦 Size (pre-GC):    " << size_before_gc / 1024 << " KB\n";
            std::cout << "  📦 Size (post-GC):   " << size_after / 1024 << " KB\n";
            std::cout << "  🗑️  Reclaimed:       " << (size_before_gc - size_after) / 1024 << " KB\n";
            std::cout << "  🛡️  Integrity:       " << correct << "/" << GC_N << "\n";
            assert(correct == GC_N);
        }
        safe_remove_all(TEST_DIR + "/gc_test");
        print_sep();

    } catch (const std::exception& e) {
        std::cerr << "❌ Benchmark failed: " << e.what() << "\n";
        safe_remove_all(TEST_DIR);
        return 1;
    } catch (...) {
        std::cerr << "❌ Benchmark failed with unknown exception.\n";
        safe_remove_all(TEST_DIR);
        return 1;
    }

    safe_remove_all(TEST_DIR);
    std::cout << "🎉 All benchmarks passed!\n";
    return 0;
}