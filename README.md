# ZeroKV

> 一个面向学习与实践的单机 KV 存储引擎：LSM-Tree 架构 + WiscKey 风格 KV 分离。
> 技术栈：C++20 · Linux · LZ4 · Zlib · CMake

![CI](https://github.com/zbyyehyehyeh/ZeroKV/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![C++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)

## 项目简介

ZeroKV 实现了现代存储引擎的核心机制：

- **写入路径**：WAL（预写日志）→ 无锁跳表 MemTable → 达到阈值后 Flush 为 SSTable（L0 → L1 Compaction）
- **KV 分离**：超过 5 字节的值走 Append-Only Value Log，元数据层只存 8 字节句柄
- **版本链**：MemTable 采用 LevelDB 式追加版本链，同 Key 多版本共存、按 seq 取最新，并发覆盖无撕裂窗口
- **崩溃一致性**：WAL + CRC32 + MANIFEST 原子替换，掉电 / `kill -9` 后可恢复
- **空间回收**：VLog 按垃圾率选文件 → 扫描重写 → Rewrite Map → Compaction → 删旧文件
- **并发**：多线程写入、Epoch-Based 内存回收、线程本地内存池、写背压控制

## 特性一览

| 模块 | 实现 |
|---|---|
| MemTable | 无锁跳表：CAS 插入 + 追加式版本链 + EpochGuard 读保护 + 线程本地内存池 |
| WAL | 变长记录 + CRC32 + 尾部撕裂截断恢复 + 轮转 |
| SSTable | LZ4 压缩块 + 重启点二分 + 前缀 Bloom Filter + mmap + 分片 LRU Block Cache |
| Value Log | Append-Only + CRC + mmap 零拷贝读 + discardable 记账 |
| GC | 按垃圾率选文件 → 暂停写 → 扫描重写 → Rewrite Map → 全局 Compaction → 删旧文件 |
| Compaction | 统一迭代器接口多路归并（同 Key 取最新 Seq，底层丢墓碑 + 旧版本统一记账） |
| 范围扫描 | DBIterator 合并 MemTable + Immutable + L0 + L1，未落盘数据可见 |
| 并发控制 | shared_mutex 分级锁 + WriteController 背压 + 后台线程池 |
| 测试 | 组件级测试 + 7 项全量基准 + 长尾延迟 + 大小 value 对比 + 火焰图 + CI（GCC/Clang、ASan/UBSan/TSan） |

## 架构图

```mermaid
flowchart LR
    Client[读写请求] --> MT[(MemTable<br/>Lock-Free SkipList)]
    Client --> WAL[(WAL<br/>CRC32)]
    MT -- 16MB 触发 Flush --> SST0[(SSTable L0)]
    SST0 -- Compaction --> SST1[(SSTable L1)]
    Client -- value > 5B --> VLOG[(Value Log<br/>Append-Only)]
    VLOG -- GC 重写 --> VLOG2[(VLog 新文件)]
    SST1 -. value 句柄 .-> VLOG
    Read[读取] --> MT
    Read --> SST0
    Read --> SST1
```

## 目录结构

```
ZeroKV/
├── include/
│   └── DB.hpp                 # 存储引擎核心 API 接口（对外暴露）
├── src/                       # 核心源码目录
│   ├── core/                  # 核心基础组件
│   │   ├── Coding.hpp         # 变长编码与解码
│   │   ├── FileUtils.hpp      # 文件 I/O 与工具函数
│   │   ├── MemPool.hpp        # 线程本地内存池 + 跨线程窃取
│   │   ├── Status.h           # 统一状态码与错误处理
│   │   └── ThreadPool.hpp     # 后台异步线程池（基于 C++20 jthread）
│   ├── iterators/             # 迭代器组件
│   │   ├── DBIterator.hpp     # 全局统一迭代器
│   │   ├── KVStoreIterator.hpp# KV 存储迭代器基类
│   │   ├── MemTableIterator.hpp# MemTable 迭代器
│   │   └── MergeIterator.hpp  # 多路归并迭代器
│   ├── memtable/              # 内存表组件
│   │   ├── EpochManager.hpp   # Epoch-Based 无锁内存回收管理器
│   │   └── LockFreeSkipList.hpp # 无锁跳表（MemTable 底层数据结构）
│   ├── process/               # 业务流程控制
│   │   ├── CompactionTask.hpp # 压缩任务（Merge + Rewrite + 旧版本记账）
│   │   ├── DataHandle.h       # 64 位值句柄位域布局（KV 分离指针）
│   │   ├── PinnableSlice.hpp  # 防悬垂安全字符串切片
│   │   └── WriteController.hpp# 写背压（Write Stall）状态机
│   ├── sstable/               # 磁盘持久化组件（SSTable）
│   │   ├── BloomFilter.hpp    # 前缀布隆过滤器（加速点查过滤）
│   │   └── SSTable.hpp        # SSTable 读写（LZ4 压缩、重启点、Block Cache、mmap）
│   └── wal/                   # 预写日志组件
│       ├── ValueLog.hpp       # KV 分离大值日志（Value Log）
│       ├── ValueLogGC.hpp     # Value Log 空间回收机制（GC）
│       └── WAL.hpp            # WAL 预写日志（CRC32 校验、崩溃恢复、轮转截断）
├── tests/                     # 测试代码目录
│   ├── scripts/
│   │   └── run_robustness_crash.sh # 极端崩溃恢复可靠性测试脚本（kill -9）
│   ├── bench.cpp              # 7 项全量基准测试（Benchmark）
│   ├── clean_perf.cpp         # 干净写/读采样负载（供火焰图，write/read 两种模式）
│   ├── latency_tests.cpp      # 长尾延迟分布（p50/p90/p99/p99.9/p99.99）
│   ├── value_size_compare.cpp # 大小 value 详细对比（inline vs VLog）
│   ├── component_tests.cpp    # 9 大核心组件逐项压测与交互测试
│   ├── robustness_tests.cpp   # 故障注入、IO 异常与并发崩溃恢复测试
│   └── unit_tests.cpp         # 原子单元测试（覆盖编码、布隆过滤器等）
├── docs/                      # 项目文档
│   └── DESIGN.md              # 核心架构与设计文档
├── .github/
│   └── workflows/
│       └── ci.yml             # GitHub Actions CI 持续集成流程（含 ASan/TSan/崩溃测试）
├── CMakeLists.txt             # CMake 构建配置
├── LICENSE                    # 项目开源协议
├── README.md                  # 项目说明文档
└── .gitignore                 # Git 忽略配置
```

## 快速开始

### 环境要求

- Linux（依赖 `mmap` / `fdatasync` / `O_DIRECTORY` 等 POSIX 接口）
- g++ ≥ 10 或 clang ≥ 12（C++20：concepts、jthread）
- CMake ≥ 3.20；liblz4-dev、zlib1g-dev

```bash
# Ubuntu / Debian 依赖
sudo apt-get install -y g++ cmake liblz4-dev zlib1g-dev

# 方式一：直接编译
g++ -O3 -std=c++20 -march=native bench.cpp -o full_bench -llz4 -lz -lpthread
./full_bench
sudo ./full_bench   # root 权限下会 drop page cache，冷读数据更准确

# 组件级测试
g++ -O2 -std=c++20 -march=native component_tests.cpp -o component_tests -llz4 -lz -lpthread
./component_tests

# 方式二：CMake
# 全部测试（单元 + 组件 + 可靠性 + 崩溃注入）
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure

# 单独运行
./build/zerokv_tests                    # 单元测试
./build/zerokv_component_tests          # 组件级测试
./build/zerokv_robustness_tests         # 可靠性专项
./build/zerokv_robustness_tests crash   # 只跑指定组（按名称前缀过滤）

# 性能专项
./build/zerokv_bench                    # 7 项全量基准
./build/zerokv_value_compare            # 大小 value 对比
./build/zerokv_latency                  # 长尾延迟分布（冷读建议 sudo）
./build/zerokv_clean_perf write         # 纯写采样（供火焰图）
./build/zerokv_clean_perf read          # 纯读采样（供火焰图）
```

### Sanitizer 构建（并发正确性）

```bash
cmake -B build-asan -DCMAKE_BUILD_TYPE=Debug -DZEROKV_SANITIZE=address,undefined
cmake --build build-asan -j$(nproc) && ./build-asan/zerokv_tests

cmake -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DZEROKV_SANITIZE=thread
cmake --build build-tsan -j$(nproc) && ./build-tsan/zerokv_tests
```

## 基准测试

7 项基准：顺序写 / 点查命中 / 点查未命中（热 vs 冷）/ 范围扫描（SST + MemTable）/ 4 线程并发写 / WAL 崩溃恢复 / VLog GC。

> 下表为 `sudo` 运行结果（冷未命中与冷扫描前已 drop page cache）。

| # | 测试项 | 数据量 | 耗时 | QPS / ops | 备注 |
|---|---|---|---|---|---|
| 1 | 顺序写 | 500,000 | 1,428.75 ms | 349,957 ops/s | |
| 2 | 点查（命中） | 200,000 | 66.83 ms | 2,992,755 QPS | 零拷贝读 |
| 3 | 点查（未命中，热） | 200,000 | 8.09 ms | 24,733,093 QPS | 误报 0 |
| 4 | 点查（未命中，冷） | 200,000 | 7.96 ms | 25,119,954 QPS | drop page cache 后 |
| 5 | 范围扫描（SST，Seek+100×Next） | 10,000 | 407.47 ms | 999,967 条 | 冷缓存 |
| 5b | 范围扫描（MemTable 未落盘） | 50,000 | 6.48 ms | — | 全量可见且有序 |
| 6 | 4 线程并发写 | 500,000 | 4,040.51 ms | 123,747 ops/s | 校验 100% |
| 7 | VLog GC | 100,000 × 1KB × 3 版本 | — | — | 回收 214,106 KB，完整性 100% |

环境：`CPU: AMD Ryzen 9 9900X（分配 4 核）| 内存: 32GB DDR5 | 磁盘: NVMe SSD（PCIe 3.0）| 系统: Ubuntu 22.04 server`

### 结果要点

- **零拷贝读**：点查走 `GetZeroCopy`（VLog 走 mmap pin，不拷贝整段），相对拷贝版 `Get` 使 1KB 大值读吞吐提升约 33%。
- **冷未命中 ≈ 热未命中**：未命中路径由前缀 Bloom 直接否定，索引块常驻进程内 BlockCache，几乎不触发磁盘 IO。
- **冷范围扫描比热慢约 4 倍**：drop page cache 后 SST 扫描 407.47 ms（热缓存约 100 ms），长尾来自 mmap 缺页 + LZ4 解压。
- **崩溃恢复**：重开实例后 10,000/10,000 全部恢复，WAL + CRC 尾部截断逻辑验证通过。
- **VLog GC**：扫描阶段用最新版本 checker 逐条校验，精确识别 200,000 条垃圾记录；回收 214,106 KB（≈ 写入量的 2/3），回收后 100,000 个 key 全部读到最新值。
- **并发写低于单线程**：写入在 VLog 与 WAL 上全局串行化（单日志顺序写设计），多线程只有锁竞争开销；group commit 已列入 Roadmap。

### 大小 value 对比（inline vs VLog）

`zerokv_value_compare` 对 value 大小逐档测写吞吐 / 写 p99 / 读 QPS / 读 p99 / 磁盘占用，覆盖 5→6 字节的分界：

| size | path | write ops/s | write p99 | read QPS | read p99 | disk KB |
|---|---|---|---|---|---|---|
| 1 | inline | 1,504,350 | 1.60 µs | 3,219,545 | 0.44 µs | 2,186 |
| 4 | inline | 1,506,015 | 1.47 µs | 3,110,392 | 0.58 µs | 2,186 |
| 5 | inline | 1,445,630 | 1.59 µs | 2,944,875 | 0.49 µs | 2,187 |
| 6 | vlog | 459,168 | 3.59 µs | 2,689,399 | 0.51 µs | 10,358 |
| 8 | vlog | 461,368 | 4.02 µs | 2,727,378 | 0.71 µs | 10,847 |
| 16 | vlog | 460,419 | 3.40 µs | 2,744,690 | 0.57 µs | 12,814 |
| 64 | vlog | 449,358 | 4.12 µs | 2,536,698 | 0.55 µs | 24,575 |
| 256 | vlog | 428,282 | 4.57 µs | 2,400,911 | 0.59 µs | 71,590 |
| 1024 | vlog | 350,984 | 4.80 µs | 2,452,673 | 0.62 µs | 259,106 |

结论：

- **5→6 字节是硬台阶**：写吞吐从 145 万掉到 46 万（-68%），磁盘从 ~2MB 跳到 10MB（VLog 开始增长），写 p99 从 1.6µs 跳到 3.6µs（+2.3×）。
- **写开销来源**：大值每次写多一次 `ValueLog::Append`（CRC + write 系统调用），小值只走 WAL。
- **读延迟几乎不随 value 大小变化**（p99 稳定在 0.5–0.7µs），印证读路径是零拷贝，读 QPS 随 size 下降主要是拷贝版 API 的 memmove 成本。

### 长尾延迟（p50 / p99 / p99.9 / p99.99）

均值会掩盖长尾；`zerokv_latency` 测延迟分布，区分 inline 与 VLog 两条路径：

```bash
./build/zerokv_latency        # 冷读建议 sudo ./build/zerokv_latency
```

| 场景 | avg | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---|---|---|---|---|---|
| 写 inline（5B） | 1.13 µs | 0.96 | 1.56 | 2.71 | 13.77 | 93.23 |
| 写 VLog（1KB） | 3.49 µs | 3.06 | 4.09 | 7.15 | 54.21 | 142.09 |
| 读命中 inline（5B） | 0.30 µs | 0.28 | 0.41 | 0.56 | 0.80 | 27.56 |
| 读命中 VLog（1KB） | 0.71 µs | 0.51 | 0.90 | 3.43 | 5.63 | 65.95 |
| 读未命中（Bloom） | 0.06 µs | 0.05 | 0.06 | 0.06 | 0.21 | 0.33 |
| 范围扫描（冷，Seek+Next×100） | 6.81 µs | 4.62 | 4.94 | 8.65 | 867.06 | 1608.40 |

（单位 µs；范围扫描为冷缓存。）

关注点：① VLog 命中 p99 比 inline 高（多一次 mmap 读 + pin）；② 冷扫描 p99.99 = 1608µs、均值 6.81µs（≈236×），长尾由 mmap 缺页 + LZ4 解压主导。

### 性能分析（火焰图）

用 `perf` + [FlameGraph](https://github.com/brendangregg/FlameGraph) 对写/读路径分别采样（`zerokv_clean_perf`，20s 纯负载）：

**写路径**：

| 函数 | 占比 | 性质 |
|---|---|---|
| `write` 系统调用（含 ext4 页缓存路径） | ~41% | 每条 WAL 记录一次 `write()` |
| `LockFreeSkipList::insert` | ~28% | 唯一的数据结构热点 |
| `__mark_inode_dirty` / `ext4_dirty_inode` | ~19% | 小写入的元数据税（每次更新 size/mtime + 日志） |
| 软件 CRC（libz） | ~8% | 可换硬件 CRC32C |

**读路径**（零拷贝）：

| 函数 | 占比 | 性质 |
|---|---|---|
| `SSTReader::Get` | ~44% | Bloom → 索引 → BlockCache → 解压 |
| `shared_mutex` 读锁 | ~11% | BlockCache 命中路径的读锁 |
| `shared_ptr` 引用计数 | ~9% | 块缓存块的持有/释放 churn |
| `LockFreeSkipList::GetHandle` | ~2.8% | 无锁跳表读，非瓶颈 |

**优化方向**（按收益排序，均已在 Roadmap / 未来展望）：

1. **WAL group commit**：批量写摊薄元数据税（19% / 批量大小）
2. **硬件 CRC32C**：替换软件 CRC，写 -7%
3. **BlockCache 命中路径去锁 + 去 shared_ptr churn**：复用 EpochManager 做读保护，读 -20%

> 采样方法：`-fno-omit-frame-pointer` 重编译 → `perf record -g --call-graph fp` → `stackcollapse-perf.pl | flamegraph.pl`。

## 可靠性测试

除基准测试外，ZeroKV 的可靠性由四层测试体系保证，全部集成进 CI（`ctest`）：

| 层级 | 内容 | 结果 |
|---|---|---|
| 单元测试 | 12 组、43,912 个断言：位域打包 / 编解码 / Bloom / WAL 往返与撕裂 / 持久化重开 / 范围扫描 / GC 完整性 / 并发写 | ✅ 0.32s 全过 |
| 组件级测试 | 9 个组件逐项正确性 + 微基准 | ✅ 0.58s 全过 |
| 可靠性专项 | 7 组定向测试：目录 fsync / 删除 / KV 分离 / 原子性 / IO 错误 / 版本链 / fork 崩溃恢复 | ✅ 1,549 断言全过 |
| 随机崩溃注入 | 随机 `kill -9` + 重启校验，committed keys 恢复率 100% | ✅ 10/10 轮通过 |

### 组件级测试结果

| 组件 | 结果 |
|---|---|
| DataHandle | 200 万次编解码 0.08 µs ✅ |
| Coding | 200 万次往返 0.02 µs ✅ |
| BloomFilter | 构建 1 万 key 0.15 ms；查询 11 万次 1.08 ms；误报率 0.837%（与理论值 ≈0.82% 一致） ✅ |
| WAL | 追加 10 万条 51.91 ms；恢复 5.42 ms；尾部撕裂截断 OK ✅ |
| LockFreeSkipList | 写 20 万 14.48 ms；读 20 万 9.07 ms；4 线程并发写读 59.52 ms ✅ |
| SSTable | 写 10 万条 6.62 ms（1,180 KB）；点查 5.63 ms；范围扫描 5 万条 0.56 ms ✅ |
| ValueLog | 追加 10 万条 147.14 ms；逐条校验读回 100,000/100,000 ✅ |
| DB + VLog GC | 2 万 key × 3 版本覆盖 + 1,000 删除；回收 13,168 KB；完整性 19,000/19,000 ✅ |
| DBIterator | MemTable 未落盘扫描 4,949/4,949 且有序 ✅ |

### 可靠性专项（7 组）

| # | 专项 | 验证点 |
|---|---|---|
| 1 | 目录 fsync / MANIFEST | 原子替换（tmp 不残留）、残留 tmp 被忽略、坏行/孤儿条目容错；`strace` 可验证 `fdatasync(tmp) → rename → fsync(目录)` 完整调用链 |
| 2 | 删除 | 大值/小值/覆盖删除、删除不存在的 key、重启 / Compaction / GC 后不复活 |
| 3 | KV 分离（VLog） | ≤5B 走 inline（VLog 零增长）、>5B 落盘且读回一致、尾部半写 CRC 截断恢复 |
| 4 | manifest/SST 原子性 | 孤儿 SST 忽略、MANIFEST 指向缺失文件跳过、损坏 SST fail-fast（拒绝打开而非返回错误数据） |
| 5 | IO 错误 | 打开失败抛异常不崩、VLog 文件丢失优雅降级（读返回 false + 新写入可用）、WAL 损坏截断 |
| 6 | 版本链 | 顺序/并发覆盖无撕裂、inline↔VLog 交替覆盖、墓碑复活、重启 / GC 后取最新 |
| 7 | 崩溃恢复（fork） | WAL 轮转截断、轮转后的写入在崩溃后完整恢复 3,500/3,500 |

### 随机崩溃注入

```bash
bash tests/scripts/run_robustness_crash.sh 10
```

## 设计文档

详见 [docs/DESIGN.md](docs/DESIGN.md)：数据格式（DataHandle / WAL / SST / VLog）、写入与读取路径、Flush 与 Compaction、GC 协议与崩溃窗口分析、恢复流程、并发控制与已知权衡。

## 已知限制

1. Key 固定为 `uint64_t`（通用 Slice/字符串 Key 在 Roadmap）
2. Bloom Filter 使用 32 位前缀语义：对"同前缀密集 key 空间"，假阳性率由前缀数量决定而非 key 数量；换来的好处是体积更小、构建去重
3. 范围扫描不提供快照隔离（扫描过程中新写入对结果的可见性无保证；单次 key/value 读取是原子的）
4. 单日志串行写：多线程写入在 VLog/WAL 全局互斥，并发写吞吐受限（Roadmap：group commit / 多段 VLog）
5. discardable 记账是启发式统计（写路径版本链 + Compaction 兜底），GC 选文件按估算比例，扫描时以 checker 逐条校验为准；崩溃后统计从零重建，不影响正确性
6. 仅支持 Linux；MANIFEST 记录绝对路径（数据库目录不可整体移动）
7. 崩溃注入目前仅覆盖单写线程场景；多线程写入过程中的崩溃尚未覆盖。断电（丢 page cache）安全性目前是设计论证 + fdatasync 语义推理，未做真实掉电实测

## 未来展望

1. 进行多线程并发优化以及多线程可靠性检测
2. 引入 io_uring 批量 IO 与 O_DIRECT（读路径替代 mmap），降低 syscall 与缺页中断带来的延迟抖动；配合 group commit、多段 VLog、分段 GC 收窄并发与 GC 造成的 p99 长尾
3. WAL 换硬件 CRC32C（替换 libz 软件 CRC），降低写路径约 7% 的 CPU

## Roadmap

- [ ] Snapshot / MVCC
- [ ] 通用 Slice Key + 字符串 Key 支持
- [ ] DeleteRange / 前缀扫描 API
- [ ] Group Commit（并发写吞吐）
- [ ] BlockCache 命中路径无锁化（复用 EpochManager）
- [ ] 与 RocksDB 的对照基准（同机器同负载）
- [ ] 基于 ZeroKV 的 Raft 分布式层

## License

MIT — 见 [LICENSE](LICENSE)

## 参考

- LevelDB / RocksDB 的 LSM 实现
- [WiscKey: Separating Keys from Values in SSD-conscious Storage (FAST '16)](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu)
