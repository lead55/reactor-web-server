#ifndef SERVER_H
#define SERVER_H

#include <sys/epoll.h>
#include <netinet/in.h>
#include <memory>
#include <vector>
#include <string>
#include <atomic>
#include "fdguard.h"
#include "http_conn.h"
#include "thread_pool.h"
#include "sub_reactor.h"

// 主从 Reactor + 线程池 架构的 Web 服务器装配类。
//   - MainReactor（本 Server 的主线程 epoll）：负责 accept 新连接
//   - Round-robin 分发给多个 SubReactor（每个独立线程 + 独立 epoll）
//   - 每个 SubReactor 把业务逻辑提交给共享的 ThreadPool
class Server {
public:
    // num_subs: SubReactor 数量（I/O 线程数）；num_workers: 线程池线程数；0=自动
    Server(int port, const std::string& root,
           int num_subs = 0, int num_workers = 0);
    ~Server();

    void start();
    void stop() noexcept { m_stop = true; }

private:
    void init_socket();
    void deal_new_conn();    // 接受新连接 + round-robin 分发

    int              m_port;
    std::string      m_root;
    int              m_num_subs;

    // -------- MainReactor：listen fd + 单 epoll（仅监听 listen socket）--------
    FdGuard          m_listenfd;
    FdGuard          m_epollfd;       // MainReactor 的 epoll
    std::atomic<bool> m_stop;
    std::vector<epoll_event> m_events;

    // -------- 从 Reactor + 线程池 --------
    // 注意：m_subs 必须在 m_pool 之前声明。
    // C++ 析构顺序与声明相反 → m_pool 先析构（join workers，保证不再有回调访问 SubReactor）
    //                         → 然后 m_subs 析构（SubReactor stop + join I/O 线程）
    std::vector<std::unique_ptr<SubReactor>> m_subs;
    ThreadPool                            m_pool;
    unsigned                              m_rr_index;   // round-robin 指针

    static constexpr int MAX_EVENTS = 128;
};

#endif // SERVER_H
