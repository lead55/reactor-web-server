#include "thread_pool.h"

ThreadPool::ThreadPool(size_t num_threads)
    : m_stop(false)
{
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) num_threads = 4;
    }
    m_workers.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        m_workers.emplace_back(&ThreadPool::worker_loop, this);
    }
    LOGI("ThreadPool started with %zu worker threads", m_workers.size());
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::shutdown() noexcept {
    bool expected = false;
    if (!m_stop.compare_exchange_strong(expected, true)) {
        return;   // 已经停止过
    }
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        // 不清理队列；析构前所有任务应已完成，这里 notify 让线程退出等待
    }
    m_cv.notify_all();
    for (auto& t : m_workers) {
        if (t.joinable()) t.join();
    }
    m_workers.clear();
    LOGI("ThreadPool shutdown complete");
}

void ThreadPool::submit(Task task) {
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_stop.load()) {
            LOGW("ThreadPool is stopping, task dropped");
            return;
        }
        m_tasks.push(std::move(task));
    }
    m_cv.notify_one();
}

void ThreadPool::worker_loop() {
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lk(m_mtx);
            m_cv.wait(lk, [this] { return m_stop.load() || !m_tasks.empty(); });
            if (m_stop.load() && m_tasks.empty()) {
                return;
            }
            task = std::move(m_tasks.front());
            m_tasks.pop();
        }
        try {
            task();
        } catch (const std::exception& e) {
            LOGE("ThreadPool task exception: %s", e.what());
        } catch (...) {
            LOGE("ThreadPool task unknown exception");
        }
    }
}
