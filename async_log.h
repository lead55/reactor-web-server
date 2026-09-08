#ifndef ASYNC_LOG_H
#define ASYNC_LOG_H

#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <cstdio>
#include <cstdarg>
#include <cstring>
#include <sys/stat.h> 
#include <ctime>      
#include <chrono>     


// -------- 日志级别 --------
enum class LogLevel { Debug, Info, Warn, Error };

inline const char* log_level_str(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO";
        case LogLevel::Warn:  return "WARN";
        case LogLevel::Error: return "ERROR";
    }
    return "UNKNOWN";
}

// -------- 固定大小日志缓冲区 --------
// 每个 buffer 4MB，减少内存分配次数；满了就换一个新 buffer
class LogBuffer {
public:
    static constexpr size_t CAPACITY = 4 * 1024 * 1024;  // 4MB

    LogBuffer() : m_data(new char[CAPACITY]), m_len(0) {}
    ~LogBuffer() { delete[] m_data; }

    // 禁止拷贝（持有堆内存）
    LogBuffer(const LogBuffer&) = delete;
    LogBuffer& operator=(const LogBuffer&) = delete;

    // 尝试追加日志；返回 false = 缓冲区不够空间
    bool append(const char* data, size_t len) {
        if (m_len + len > CAPACITY) return false;
        memcpy(m_data + m_len, data, len);
        m_len += len;
        return true;
    }

    void clear() noexcept { m_len = 0; }
    const char* data() const noexcept { return m_data; }
    size_t      size() const noexcept { return m_len; }
    bool        empty() const noexcept { return m_len == 0; }

private:
    char*  m_data;
    size_t m_len;
};


class AsyncLog {
public:
    // Meyers' Singleton：C++11 保证线程安全初始化
    static AsyncLog& instance() {
        static AsyncLog log;
        return log;
    }

    // 设置日志目录和基础文件名（默认 ./log/server.log）
    // echo_stderr_level: 同步输出到 stderr 的最低级别阈值
    //   - LogLevel::Debug : 所有级别都打印到终端
    //   - LogLevel::Info  : INFO/WARN/ERROR 打印到终端
    //   - LogLevel::Warn  : 仅 WARN/ERROR 打印
    //   - LogLevel::Error : 仅 ERROR 打印
    //   - 传 static_cast<LogLevel>(99) 或更高值 = 完全关闭终端输出
    // 必须在首次写日志前调用；若未调用，使用默认值 Debug
    void init(const std::string& dir = "./log",
              const std::string& base_name = "server.log",
              LogLevel level = LogLevel::Debug,
              LogLevel echo_stderr_level = LogLevel::Debug) {
        std::lock_guard<std::mutex> lk(m_mtx);
        m_dir             = dir;
        m_base_name       = base_name;
        m_level           = level;
        m_echo_stderr_level = echo_stderr_level;
        m_started         = true;
        // 创建日志目录（使用系统调用，避免 system() 命令注入风险）
        ::mkdir(m_dir.c_str(), 0755);
        // 打开今天的日志文件
        roll_file_locked();
        // 启动后端线程
        m_thread = std::thread(&AsyncLog::backend_loop, this);
    }

    // 前端接口：格式化并追加一条日志到 current buffer
    void append(LogLevel level, const char* file, int line, const char* fmt, ...) {
        if (static_cast<int>(level) < static_cast<int>(m_level.load())) return;

        // 1) 格式化时间戳 + 级别 + 文件名 + 行号
        char header[256];
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        struct tm tmbuf;
        struct tm* t = localtime_r(&ts.tv_sec, &tmbuf);
        char time_buf[32];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", t);

        const char* slash = strrchr(file, '/');
        const char* fname = slash ? slash + 1 : file;

        int hlen = snprintf(header, sizeof(header), "[%s][%s][%s:%d] ",
                            time_buf, log_level_str(level), fname, line);
        if (hlen < 0) hlen = 0;
        if (hlen > (int)sizeof(header) - 1) hlen = sizeof(header) - 1;

        // 2) 格式化用户消息
        char msg[1024];
        va_list args;
        va_start(args, fmt);
        int mlen = vsnprintf(msg, sizeof(msg), fmt, args);
        va_end(args);
        if (mlen < 0) mlen = 0;
        if (mlen > (int)sizeof(msg) - 1) mlen = sizeof(msg) - 1;

        // 3) 拼接成一行 + '\n'
        size_t total = static_cast<size_t>(hlen) + static_cast<size_t>(mlen) + 1;
        char line_buf[256 + 1024];
        memcpy(line_buf, header, static_cast<size_t>(hlen));
        memcpy(line_buf + hlen, msg, static_cast<size_t>(mlen));
        line_buf[total - 1] = '\n';

        // 4') 开发便利：>= echo 阈值的日志同步输出到 stderr（低概率级别不影响吞吐）
        if (static_cast<int>(level) >= static_cast<int>(m_echo_stderr_level.load())) {
            fwrite(line_buf, 1, total, stderr);
            fflush(stderr);
        }

        // 4) 追加到 current buffer（加锁，临界区极短：仅 memcpy）
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_started.load()) return; 
        if (!m_cur_buf->append(line_buf, total)) {
            // current buffer 满：移到 ready 队列，换新 buffer
            m_ready_bufs.push_back(std::move(m_cur_buf));
            m_cur_buf.reset(new LogBuffer());
            m_cur_buf->append(line_buf, total);
        }
        m_cond.notify_one();   // 唤醒后端线程
    }

    // 停止日志线程，flush 剩余缓冲
    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (!m_started) return;
            m_started = false;
            m_cond.notify_all();
        }
        if (m_thread.joinable()) m_thread.join();
    }

    ~AsyncLog() {
        shutdown();
    }

private:
    AsyncLog()
        : m_cur_buf(new LogBuffer()),
          m_dir("./log"),
          m_base_name("server.log"),
          m_level(LogLevel::Debug),
          m_echo_stderr_level(LogLevel::Debug),
          m_started(false),
          m_day(0) {}

    // -------- 后端线程主循环 --------
    void backend_loop() {
        while (true) {
            std::unique_lock<std::mutex> lk(m_mtx);
            // 等待：缓冲区满 或 超时 1 秒 或 shutdown
            m_cond.wait_for(lk, std::chrono::seconds(1), [this] {
                return !m_ready_bufs.empty() || !m_started;
            });

            // swap current → ready（即使 current 未满，也一起刷盘，保证低延迟）
            if (!m_cur_buf->empty()) {
                m_ready_bufs.push_back(std::move(m_cur_buf));
                m_cur_buf.reset(new LogBuffer());
            }

            // 把 ready buffer 列表移到本地（减小临界区）
            std::vector<std::unique_ptr<LogBuffer>> bufs;
            bufs.swap(m_ready_bufs);

            lk.unlock();

            // 写文件（无锁，不阻塞前端）
            for (auto& buf : bufs) {
                write_to_file(buf->data(), buf->size());
                buf->clear();
            }

            if (!m_started) {
                // shutdown：flush 剩余后退出
                lk.lock();
                if (!m_cur_buf->empty()) {
                    write_to_file(m_cur_buf->data(), m_cur_buf->size());
                    m_cur_buf->clear();
                }
                if (m_fp && m_fp != stderr) { fflush(m_fp); fclose(m_fp); m_fp = nullptr; }
                break;
            }
            fflush(m_fp);
        }
    }

    // -------- 文件滚动（按天）+ 写入 --------
    void write_to_file(const char* data, size_t len) {
        // 检查是否跨天
        time_t now = time(nullptr);
        struct tm tmbuf;
        struct tm* t = localtime_r(&now, &tmbuf);
        if (t->tm_mday != m_day) {
            roll_file_locked();
        }
        if (!m_fp) roll_file_locked();
        if (m_fp) {
            fwrite(data, 1, len, m_fp);
        }
    }

    // 打开/滚动日志文件（调用者需持锁）
    void roll_file_locked() {
        // 不要 fclose(stderr)：它是进程级共享流，关闭后所有 fprintf(stderr,...) 失效
        if (m_fp && m_fp != stderr) { fflush(m_fp); fclose(m_fp); }
        m_fp = nullptr;

        time_t now = time(nullptr);
        struct tm tmbuf;
        struct tm* t = localtime_r(&now, &tmbuf);
        m_day = t->tm_mday;

        char date[32];
        strftime(date, sizeof(date), "%Y-%m-%d", t);
        // 文件名格式：dir/base_name.YYYY-MM-DD
        m_filename = m_dir + "/" + m_base_name + "." + date;
        m_fp = fopen(m_filename.c_str(), "a");
        if (!m_fp) {
            // 回退到 stderr
            m_fp = stderr;
        }
    }

    // -------- 成员 --------
    std::mutex      m_mtx;
    std::condition_variable m_cond;

    std::unique_ptr<LogBuffer> m_cur_buf;           // 前端正在写的 buffer
    std::vector<std::unique_ptr<LogBuffer>> m_ready_bufs;  // 待刷盘的 buffer 列表

    std::string     m_dir;          // 日志目录
    std::string     m_base_name;    // 基础文件名
    std::atomic<LogLevel> m_level;  // 日志级别过滤（写文件的最低级别）
    std::atomic<LogLevel> m_echo_stderr_level;  // 同步输出到 stderr 的最低级别阈值
    std::atomic<bool> m_started;    // 后端线程运行标志
    int             m_day;          // 当前日期（用于跨天滚动检测）

    std::thread     m_thread;
    FILE*           m_fp = nullptr; // 当前日志文件
    std::string     m_filename;     // 当前文件名
};

// log.h 的宏接口：LOGI / LOGW / LOGE / LOGD
// 调用 AsyncLog::instance().append()
#define LOGD(fmt, ...) AsyncLog::instance().append(LogLevel::Debug, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOGI(fmt, ...) AsyncLog::instance().append(LogLevel::Info,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOGW(fmt, ...) AsyncLog::instance().append(LogLevel::Warn,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOGE(fmt, ...) AsyncLog::instance().append(LogLevel::Error, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif // ASYNC_LOG_H
