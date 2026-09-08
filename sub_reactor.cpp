#include "sub_reactor.h"
#include "thread_pool.h"
#include "async_log.h"
#include <cerrno>
#include <ctime>
#include <arpa/inet.h>
#include <unistd.h>

// CLOCK_MONOTONIC 当前时间（秒），不受 NTP/系统时间调整影响
int64_t SubReactor::now_monotonic() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec);
}

SubReactor::SubReactor(int id, ThreadPool& pool)
    : m_id(id),
      m_pool(pool),
      m_epollfd(-1),
      m_eventfd(-1),
      m_timerfd(-1),
      m_stop(false),
      m_users(std::make_unique<http_conn[]>(http_conn::MAX_FD)),
      m_events(MAX_EVENTS),
      m_timers(http_conn::MAX_FD)
{
    m_epollfd.reset(epoll_create(5));
    if (!m_epollfd) {
        LOGE("SubReactor[%d] epoll_create failed: %s", m_id, strerror(errno));
    }
    // eventfd：EFD_NONBLOCK + EFD_CLOEXEC
    m_eventfd.reset(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (!m_eventfd) {
        LOGE("SubReactor[%d] eventfd failed: %s", m_id, strerror(errno));
    } else {
        http_conn::addfd(m_epollfd.get(), m_eventfd.get(), false);
    }
    // timerfd：CLOCK_MONOTONIC + 非阻塞，用于驱动小根堆超时检查
    m_timerfd.reset(timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC));
    if (!m_timerfd) {
        LOGE("SubReactor[%d] timerfd_create failed: %s", m_id, strerror(errno));
    } else {
        http_conn::addfd(m_epollfd.get(), m_timerfd.get(), false);
    }
    LOGI("SubReactor[%d] created (epollfd=%d, eventfd=%d, timerfd=%d)",
         m_id, m_epollfd.get(), m_eventfd.get(), m_timerfd.get());
}

SubReactor::~SubReactor() {
    stop();
    LOGI("SubReactor[%d] destroyed", m_id);
}

void SubReactor::start() {
    if (m_thread.joinable()) return;
    m_thread = std::thread(&SubReactor::loop, this);
    LOGI("SubReactor[%d] thread started", m_id);
}

void SubReactor::stop() {
    bool expected = false;
    if (!m_stop.compare_exchange_strong(expected, true)) {
        // 已经 stop 过，确保 join 一次
        if (m_thread.joinable()) m_thread.join();
        return;
    }
    wakeup();   // 唤醒 loop 退出
    if (m_thread.joinable()) m_thread.join();
}

// -------- 跨线程安全：加锁 + eventfd 唤醒 --------

void SubReactor::wakeup() {
    if (!m_eventfd) return;
    uint64_t one = 1;
    // write eventfd，注意 EAGAIN（计数满），忽略即可（有至少一个计数就会触发 EPOLLIN）
    ssize_t s = ::write(m_eventfd.get(), &one, sizeof(one));
    (void)s;
}

void SubReactor::drain_eventfd() {
    if (!m_eventfd) return;
    uint64_t buf;
    while (true) {
        ssize_t s = ::read(m_eventfd.get(), &buf, sizeof(buf));
        if (s > 0) continue;
        if (s < 0 && errno == EAGAIN) break;
        if (s < 0 && errno == EINTR)  continue;
        break;
    }
}

// -------- 定时器相关 --------

void SubReactor::drain_timerfd() {
    if (!m_timerfd) return;
    uint64_t exp;
    while (true) {
        ssize_t s = ::read(m_timerfd.get(), &exp, sizeof(exp));
        if (s > 0) continue;
        if (s < 0 && errno == EAGAIN) break;
        if (s < 0 && errno == EINTR)  continue;
        break;
    }
}

void SubReactor::arm_timerfd() {
    if (!m_timerfd) return;
    int64_t now = now_monotonic();
    int64_t next = m_timers.next_expire();
    struct itimerspec its = {};
    if (next < 0) {
        // 无定时器：disarm
        its.it_value = {0, 0};
    } else {
        int64_t delta = next - now;
        if (delta <= 0) delta = 1;   // 已超时，至少 1 秒后触发（避免忙等）
        its.it_value.tv_sec  = delta;
        its.it_value.tv_nsec = 0;
        its.it_interval = {0, 0};     // 单次触发（每次 tick 后重新 arm）
    }
    timerfd_settime(m_timerfd.get(), 0, &its, nullptr);
}

void SubReactor::handle_timeout() {
    drain_timerfd();
    int64_t now = now_monotonic();
    m_timers.tick(now, [this](int fd) {
        LOGI("SubReactor[%d] connection fd=%d timeout, closing", m_id, fd);
        close_connection(fd);
    });
    arm_timerfd();   // 重新设置下一次超时
}

void SubReactor::dispatch_new_conn(int cfd, const sockaddr_in& addr) {
    SubMsg msg;
    msg.type = MsgType::NewConn;
    msg.cfd = cfd;
    msg.addr = addr;
    msg.bus_result = 0;
    {
        std::lock_guard<std::mutex> lk(m_msg_mtx);
        m_msgs.push(msg);
    }
    wakeup();
}

void SubReactor::notify_bus_done(int cfd, int result) {
    SubMsg msg;
    msg.type = MsgType::BusDone;
    msg.cfd = cfd;
    msg.addr = sockaddr_in{};
    msg.bus_result = result;
    {
        std::lock_guard<std::mutex> lk(m_msg_mtx);
        m_msgs.push(msg);
    }
    wakeup();
}

void SubReactor::swap_msgs(std::queue<SubMsg>& out) {
    std::lock_guard<std::mutex> lk(m_msg_mtx);
    std::swap(out, m_msgs);
}

// -------- SubReactor 内部（单线程，无锁） --------

void SubReactor::add_new_conn(int cfd, const sockaddr_in& addr) {
    if (cfd >= http_conn::MAX_FD) {
        ::close(cfd);
        LOGW("SubReactor[%d] fd=%d exceed MAX_FD", m_id, cfd);
        return;
    }
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    LOGI("SubReactor[%d] accept client %s:%d fd=%d",
         m_id, ip, ntohs(addr.sin_port), cfd);
    m_users[cfd].init(cfd, addr);
    http_conn::addfd(m_epollfd.get(), cfd, true);
    // 注册定时器：15 秒无数据交互则自动关闭
    m_timers.add(cfd, TIMEOUT_SEC, now_monotonic());
    arm_timerfd();
}

void SubReactor::deal_read(int fd) {
    http_conn& conn = m_users[fd];
    if (!conn.read_once()) {
        close_connection(fd);
        return;
    }
    // 有数据交互：续期定时器
    m_timers.refresh(fd, TIMEOUT_SEC, now_monotonic());
    arm_timerfd();

    int r = conn.prepare_io_read();
    if (r == 0) {
        // 请求不完整：重新注册 EPOLLIN
        http_conn::modfd(m_epollfd.get(), fd, EPOLLIN);
    } else {
        // 请求（或错误码）解析完成：提交业务到线程池
        // 安全：先 invalidate 定时器，防止业务处理期间定时器超时关闭连接（竞态）
        m_timers.invalidate(fd);
        SubReactor* self = this;
        m_pool.submit([self, fd] {
            int result = 2;   // 默认失败：关闭
            try {
                http_conn& c = self->m_users[fd];
                if (c.get_fd() == fd) {   // 防御：连接没被提前关闭
                    result = c.run_business_and_prepare_write();
                }
            } catch (const std::exception& e) {
                LOGE("SubReactor[%d] bus task exception fd=%d: %s",
                     self->m_id, fd, e.what());
                result = 2;
            } catch (...) {
                LOGE("SubReactor[%d] bus task unknown exception fd=%d",
                     self->m_id, fd);
                result = 2;
            }
            // 业务完成后通知 SubReactor（线程安全，内部 eventfd 唤醒）
            self->notify_bus_done(fd, result);
        });
        // 注意：由于 addfd 时用了 EPOLLONESHOT，此时 fd 已自动从"就绪集合"disable，
        // 不会再触发任何 I/O 事件，直到 notify_bus_done 回来 modfd。
        // 因此 SubReactor 线程与业务线程不会并发访问同一个 http_conn —— 竞态安全。
    }
}

void SubReactor::deal_write(int fd) {
    http_conn& conn = m_users[fd];
    // 可写事件 = 有 I/O 活动：续期定时器
    m_timers.refresh(fd, TIMEOUT_SEC, now_monotonic());
    arm_timerfd();

    int r = conn.write();
    if (r == 0) {
        close_connection(fd);
        return;
    }
    if (r == 2) {
        // 发送缓冲区满：保持 EPOLLOUT，等待下次可写
        http_conn::modfd(m_epollfd.get(), fd, EPOLLOUT);
        return;
    }
    // r == 1：本次响应已全部发送完毕
    if (conn.keep_alive()) {
        bool has_pipeline = conn.reset_for_next();
        if (has_pipeline) {
            // pipeline：缓冲区中已有下一个请求的数据，立即解析
            int pr = conn.prepare_io_read();
            if (pr == 1) {
                // 已有完整请求（或错误码）：提交业务
                // 安全：先 invalidate 定时器，防止业务处理期间定时器超时关闭连接
                m_timers.invalidate(fd);
                SubReactor* self = this;
                m_pool.submit([self, fd] {
                    int result = 2;
                    try {
                        http_conn& c = self->m_users[fd];
                        if (c.get_fd() == fd) {
                            result = c.run_business_and_prepare_write();
                        }
                    } catch (const std::exception& e) {
                        LOGE("SubReactor[%d] pipeline bus task exception fd=%d: %s",
                             self->m_id, fd, e.what());
                        result = 2;
                    } catch (...) {
                        LOGE("SubReactor[%d] pipeline bus task unknown exception fd=%d",
                             self->m_id, fd);
                        result = 2;
                    }
                    self->notify_bus_done(fd, result);
                });
                return;   // EPOLLONESHOT 已 disable，等待 notify
            } else {
                // pr == 0：pipeline 数据不全，等待后续 EPOLLIN
                http_conn::modfd(m_epollfd.get(), fd, EPOLLIN);
                return;
            }
        } else {
            // 无残留：等待下一个请求的 EPOLLIN
            http_conn::modfd(m_epollfd.get(), fd, EPOLLIN);
            return;
        }
    } else {
        close_connection(fd);
    }
}

void SubReactor::close_connection(int fd) {
    http_conn::removefd(m_epollfd.get(), fd);
    m_users[fd].close_conn();
    // 使该 fd 的所有 pending 定时器节点失效
    m_timers.invalidate(fd);
}

void SubReactor::handle_msg(const SubMsg& msg) {
    switch (msg.type) {
    case MsgType::NewConn: {
        add_new_conn(msg.cfd, msg.addr);
        break;
    }
    case MsgType::BusDone: {
        int fd = msg.cfd;
        // 防御性检查：fd 合法性 + 连接未被提前 close
        if (fd < 0 || fd >= http_conn::MAX_FD || m_users[fd].get_fd() != fd) {
            LOGD("SubReactor[%d] bus_done but fd=%d already closed", m_id, fd);
            return;
        }
        if (msg.bus_result == 1) {
            // 响应就绪：注册 EPOLLOUT（同时 EPOLLONESHOT，保持单线程语义）
            http_conn::modfd(m_epollfd.get(), fd, EPOLLOUT);
            // 重新注册定时器（之前 submit 业务时 invalidate 过）
            m_timers.add(fd, TIMEOUT_SEC, now_monotonic());
            arm_timerfd();
        } else {
            // 失败：关闭连接
            close_connection(fd);
        }
        break;
    }
    }
}

void SubReactor::loop() {
    LOGI("SubReactor[%d] event loop started", m_id);
    while (!m_stop.load()) {
        int n = epoll_wait(m_epollfd.get(), m_events.data(),
                           static_cast<int>(m_events.size()), -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOGE("SubReactor[%d] epoll_wait error: %s", m_id, strerror(errno));
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = m_events[i].data.fd;
            if (fd == m_eventfd.get()) {
                // eventfd 触发：消费 + 处理所有消息
                drain_eventfd();
                std::queue<SubMsg> q;
                swap_msgs(q);
                while (!q.empty()) {
                    handle_msg(q.front());
                    q.pop();
                }
            } else if (fd == m_timerfd.get()) {
                // timerfd 触发：检查小根堆，关闭超时连接
                handle_timeout();
            } else {
                // ===== 客户端 fd：关键！事件处理顺序修正 =====
                //   之前的 Bug: RDHUP 在 IN 之前判断，会导致"FIN + 请求数据同时到达"时
                //   直接 close_connection，请求根本没 parse，响应被吞
                //   正确顺序: 先 IN/OUT（读/写数据+回响应），最后 RDHUP/HUP/ERR（断开兜底）
                //   全部拆成独立 if（不是 else if），因为 FIN+数据同时到时 flags 会同时置位

                uint32_t ev = m_events[i].events;

                if (ev & EPOLLIN) {
                    deal_read(fd);
                    // 注意：deal_read 内可能两种结局：
                    //   1) read_once 返回 false → close_connection 已调（m_users[fd].get_fd() == -1）
                    //   2) 提交业务线程 → fd 因 EPOLLONESHOT 被 disable，后续等 BusDone
                    //   无论哪种，后续 RDHUP 判断都有 get_fd()==fd 防御，不会重复关
                }
                if (m_users[fd].get_fd() == fd && (ev & EPOLLOUT)) {
                    deal_write(fd);
                }
                // ===== 断开/错误：最后兜底 =====
                //  不要因为 EPOLLRDHUP 直接 close_connection！
                //     EPOLLRDHUP 只是"对方关了写端 (half-close / FIN)"，读端还开着等响应
                //     （比如客户端"发完请求立刻 FIN，但还等着收 400/404/200"就是这种场景）
                //     如果这里提前关，响应就发不出去了（刚才你看到的"只有 LOGW 没 respond"）
                //  真正触发 close 的应该是：
                //     deal_read 内 read_once 返回 false（FIN 且没残余数据）
                //     deal_write 后 keep_alive=false（例如 400/403/500 强制关）
                //     定时器超时空闲 15 秒
                //  只对真正不可恢复的 HUP/ERR 立刻 close
                if (m_users[fd].get_fd() == fd && (ev & (EPOLLHUP | EPOLLERR))) {
                    LOGW("SubReactor[%d] fd=%d unrecoverable event events=%u, closing",
                         m_id, fd, (unsigned)ev);
                    close_connection(fd);
                }
            }
        }
    }
    LOGI("SubReactor[%d] event loop stopped", m_id);
}
