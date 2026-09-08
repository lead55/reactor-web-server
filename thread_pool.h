#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <vector>
#include "async_log.h"

// 业务处理（HTTP 请求解析、文件读取、响应生成）放在线程池里，避免阻塞 I/O 线程。
class ThreadPool {
public:
    using Task = std::function<void()>;

    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 提交任务，线程安全
    void submit(Task task);

    // 停止线程池（析构自动调用）
    void shutdown() noexcept;

private:
    void worker_loop();

    std::vector<std::thread> m_workers;
    std::queue<Task>         m_tasks;
    std::mutex               m_mtx;
    std::condition_variable  m_cv;
    std::atomic<bool>        m_stop;
};

#endif // THREAD_POOL_H
