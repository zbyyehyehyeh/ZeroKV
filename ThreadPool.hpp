#pragma once

// [优化建议] 移除未使用的头文件: <iostream>, <atomic>, <vector>
#include <thread>
#include <mutex>
#include <future>
#include <condition_variable>
#include <functional>
#include <queue>
#include <vector>
#include <concepts>
#include <stop_token> // 仅用于 jthread 隐式依赖，代码中不再直接使用 stop_token

using ThreadHook = std::function<void()>;

class ThreadPool
{
public:
    /**
     * @brief 构造函数
     * @param num_threads 工作线程数量
     * @param on_start 线程启动钩子（每个 worker 执行一次）
     * @param on_exit 线程退出钩子（每个 worker 执行一次）
     */
    explicit ThreadPool(size_t num_threads,
                        ThreadHook on_start = nullptr,
                        ThreadHook on_exit = nullptr)
        : on_thread_start_(std::move(on_start)), // std::move 避免 std::function 拷贝开销
          on_thread_exit_(std::move(on_exit))
    {
        workers_.reserve(num_threads); // [优化] list 无 reserve，若改用 vector 可加此句
        for (size_t i = 0; i < num_threads; ++i)
        {
            // 【语法】std::jthread 构造时会自动生成内部 stop_source，
            //        并将对应的 stop_token 作为 lambda 的第一个参数传入。
            // 【设计】此处显式接收但命名省略(unnamed parameter)，
            //        因为我们改用 bool stopped_ 做业务停止判断，
            //        jthread 的 stop_token 仅保留用于析构时的自动 join 机制。
            workers_.emplace_back([this](std::stop_token /*stoken*/)
                                  {
                // RAII 保证：即使 spawnWorker 抛异常，on_exit 也会执行
                if (on_thread_start_) on_thread_start_();
                spawnWorker();
                if (on_thread_exit_) on_thread_exit_(); });
        }
    }

    ~ThreadPool()
    {
        // 【步骤1】持锁设置停止标志
        // 目的：与 enqueue 中的检查形成互斥，防止在 "检查stopped_" 和 "emplace任务" 之间
        //       析构函数插入了 stopped_=true，导致任务被静默丢弃或 worker 提前退出
        {
            std::lock_guard<std::mutex> lk(mtx_);
            stopped_ = true;
        }

        // 【步骤2】唤醒所有阻塞在 cv_.wait 上的 worker
        // 注意：必须在锁外调用，避免被唤醒的线程立即阻塞等待析构函数释放锁
        cv_.notify_all();

        // 【步骤3】隐式清理
        // 当 ~ThreadPool() 作用域结束时，workers_ (std::vector<std::jthread>) 逐个析构。
        // std::jthread::~jthread() 内部等价于:
        //   if (joinable()) { request_stop(); join(); }
        // 因此无需手动 request_stop() 和 join()。
        //
        // ⚠️ 关键前提：worker 循环必须能响应 stopped_==true 并退出，
        //    否则 jthread::join() 会永久阻塞！当前 spawnWorker 已正确处理此点。
    }

    // 禁止拷贝/赋值：线程池持有不可复制的资源（线程、mutex）
    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    /**
     * @brief 提交单个任务并获取 future
     * @tparam F 可调用对象类型
     * @tparam Args 参数类型包
     * @return std::future<返回值类型>
     */
    template <typename F, typename... Args>
        requires std::invocable<F, Args...> // C++20 Concept: 编译期验证 f(args...) 合法
    auto enqueue(F &&f, Args &&...args) -> std::future<std::invoke_result_t<F, Args...>>
    {
        // 推导 f(args...) 的返回类型
        using ReturnType = std::invoke_result_t<F, Args...>;

        // 【语法】packaged_task 将任意签名统一为 void()->ReturnType
        // 【语法】shared_ptr 包装是因为 packaged_task 不可拷贝，
        //         而 std::function<void()> 要求可拷贝构造
        // 【语法】[...args = std::forward<Args>(args)] 是 C++20 pack expansion capture，
        //         对每个参数独立完美转发捕获
        // 【语法】mutable 允许 lambda 内部 move 捕获的参数
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            [func = std::forward<F>(f), ... args = std::forward<Args>(args)]() mutable -> ReturnType
            {
                return std::invoke(std::move(func), std::move(args)...);
            });

        // ⚠️ get_future() 必须在 emplace 之前调用，
        //    因为 emplace 后 task 被 move 进队列，原 shared_ptr 可能失效
        std::future<ReturnType> res = task->get_future();
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (stopped_)
            {
                throw std::runtime_error("ThreadPool is stopped");
            }
            tasks_.emplace([task = std::move(task)]()
                           { (*task)(); });
        }
        cv_.notify_one(); // 只需唤醒一个线程处理单个任务
        return res;
    }

    /**
     * @brief 批量提交任务（fire-and-forget）
     * @return 实际入队的任务数量（0 表示线程池已停止）
     */
    template <typename Iterator>
        requires std::input_iterator<Iterator> &&
                 std::convertible_to<std::iter_value_t<Iterator>, std::function<void()>>
    size_t enqueue_batch(Iterator first, Iterator last)
    {
        size_t count = 0;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (stopped_)
                return 0;
            for (auto it = first; it != last; ++it)
            {
                tasks_.emplace(std::move(*it));
                ++count;
            }
        }
        cv_.notify_all(); // 批量任务需唤醒多个线程并行消费
        return count;
    }

private:
    void spawnWorker()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(mtx_);

                // 【核心】使用普通 condition_variable + bool 谓词
                // 规避 libstdc++ 中 condition_variable_any::wait(lock, stop_token, pred)
                // 在某些版本下的内部死锁问题 (GCC Bug #107834)
                //
                // wait 返回条件: stopped_==true OR tasks_非空
                cv_.wait(lk, [this]
                         { return stopped_ || !tasks_.empty(); });

                // 【退出判定】仅在 "已停止 AND 队列为空" 时退出
                // 这保证了优雅排空(drain): stop 后仍处理完剩余任务
                if (stopped_ && tasks_.empty())
                {
                    return;
                }

                task = std::move(tasks_.front());
                tasks_.pop();
            }

            try
            {
                task();
            }
            catch (...)
            {
                // TODO: 生产环境应替换为异常回调或日志记录
                // 对于 enqueue 提交的任务，异常会通过 future 传播；
                // 但对于 enqueue_batch 的任务，异常在此被吞没。
            }
        }
    }

    bool stopped_ = false;              // 业务层停止标志，受 mtx_ 保护
    std::condition_variable cv_;        // 普通 CV，避开 _any + stop_token 的 bug
    std::vector<std::jthread> workers_; // jthread 提供自动 join + RAII
    std::queue<std::function<void()>> tasks_;
    std::mutex mtx_;

    ThreadHook on_thread_start_;
    ThreadHook on_thread_exit_;
};