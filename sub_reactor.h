#ifndef SUB_REACTOR_H
#define SUB_REACTOR_H

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <netinet/in.h>
#include <thread>
#include <mutex>
#include <memory>
#include <vector>
#include <queue>
#include <atomic>
#include "fdguard.h"
#include "http_conn.h"
#include "timer_heap.h"

class ThreadPool;  // 前向声明

// -------- SubReactor 跨线程消息 --------
// MsgType::NewConn  : 来自 MainReactor，分发一个新连接到此 SubReactor
// MsgType::BusDone  : 来自 ThreadPool，业务处理完成，响应就绪或需要关闭
enum class MsgType { NewConn, BusDone };

struct SubMsg {
    MsgType     type;
    int         cfd;          // 连接 fd
    sockaddr_in addr;         // NewConn 时使用
    int         bus_result;   // BusDone 时使用: 1=响应就绪(mod EPOLLOUT) 2=需要关闭
};

// SubReactor：运行在独立线程，拥有独立 epoll。
//   - 通过 eventfd 被 MainReactor / ThreadPool 唤醒
//   - 处理所属连接的 ET 读写
//   - 读到完整请求后，把业务逻辑提交 ThreadPool 执行
class SubReactor {
public:
    // 注意：id 仅用于日志，pool 引用由外部持有（生命周期长于 SubReactor）
    SubReactor(int id, ThreadPool& pool);
    ~SubReactor();

    SubReactor(const SubReactor&) = delete;
    SubReactor& operator=(const SubReactor&) = delete;

    // 启动线程（内部调用 loop）
    void start();
    // 停止并 join
    void stop();

    // ---- 跨线程调用接口（线程安全，内部加锁 + eventfd 唤醒）----
    // 分发一个新连接（由 MainReactor 调用）
    void dispatch_new_conn(int cfd, const sockaddr_in& addr);
    // 业务处理完成通知（由 ThreadPool 任务回调调用）
    void notify_bus_done(int cfd, int result);

    // SubReactor 的 index/id（日志用）
    int id() const noexcept { return m_id; }

private:
    // 唤醒 eventfd
    void wakeup();
    // 消费 eventfd 计数
    void drain_eventfd();
    // 取出所有待处理消息（加锁 swap 到局部队列，减少锁持有）
    void swap_msgs(std::queue<SubMsg>& out);
    // 处理一条消息
    void handle_msg(const SubMsg& msg);

    void add_new_conn(int cfd, const sockaddr_in& addr);
    void deal_read(int fd);
    void deal_write(int fd);
    void close_connection(int fd);

    // -------- 定时器相关 --------
    void drain_timerfd();         // 消费 timerfd 超时计数
    void arm_timerfd();           // 根据 next_expire 重新设置 timerfd
    void handle_timeout();        // tick 小根堆 + 关闭超时连接 + 重新 arm

    static int64_t now_monotonic();  // CLOCK_MONOTONIC 当前秒

    void loop();

    int                m_id;
    ThreadPool&        m_pool;

    FdGuard            m_epollfd;
    FdGuard            m_eventfd;   // 跨线程唤醒
    FdGuard            m_timerfd;   // 定时器 fd，驱动小根堆超时检查
    std::atomic<bool>  m_stop;
    std::thread        m_thread;

    std::mutex                m_msg_mtx;
    std::queue<SubMsg>        m_msgs;

    // 连接表：按 fd 索引，每个 SubReactor 独立一份（fd 全局唯一不冲突）
    std::unique_ptr<http_conn[]> m_users;
    std::vector<epoll_event>     m_events;

    // 小根堆定时器：管理本 SubReactor 所有连接的空闲超时
    TimerHeap                    m_timers;
    static constexpr int         TIMEOUT_SEC = 15;  // 空闲 15 秒自动关闭

    static constexpr int MAX_EVENTS = 1024;
};

#endif // SUB_REACTOR_H
