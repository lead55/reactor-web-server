#include "server.h"
#include "async_log.h"
#include <cerrno>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <thread>   // hardware_concurrency

Server::Server(int port, const std::string& root, int num_subs, int num_workers)
    : m_port(port),
      m_root(root),
      m_num_subs(num_subs > 0 ? num_subs
                 : (std::thread::hardware_concurrency() > 0
                    ? static_cast<int>(std::thread::hardware_concurrency())
                    : 4)),
      m_listenfd(-1),
      m_epollfd(-1),
      m_stop(false),
      m_events(MAX_EVENTS),
      // 注意：成员初始化顺序 = 声明顺序：m_subs（空 vector）→ m_pool（构造好）
      // 因此 SubReactor 不能放在初始化列表，必须在 body 中创建（此时 m_pool 已就绪）
      m_pool(static_cast<size_t>(num_workers)),
      m_rr_index(0)
{
    http_conn::set_root(root);

    // MainReactor 自己的 epoll（只管 listen socket）
    m_epollfd.reset(epoll_create(5));
    if (!m_epollfd) {
        LOGE("MainReactor epoll_create failed: %s", strerror(errno));
    }

    // 创建 SubReactor（每个拥有独立线程 + 独立 epoll + eventfd）
    // 重要：m_pool 此时已经按声明顺序初始化完毕，引用有效
    m_subs.reserve(static_cast<size_t>(m_num_subs));
    for (int i = 0; i < m_num_subs; ++i) {
        m_subs.push_back(std::make_unique<SubReactor>(i, m_pool));
    }
    LOGI("Server created: %d SubReactors (I/O threads) + shared ThreadPool",
         m_num_subs);
}

Server::~Server() {
    m_stop.store(true);
    // C++ 析构顺序与声明相反：m_pool 先析构（join workers，保证不再有回调访问 SubReactor）
    // → 然后 m_subs 析构（SubReactor stop + join I/O 线程）
    // 这正是 server.h 中 m_subs 在 m_pool 之前声明的目的
}

void Server::init_socket() {
    m_listenfd.reset(socket(PF_INET, SOCK_STREAM, 0));
    if (!m_listenfd) {
        LOGE("socket failed: %s", strerror(errno));
        return;
    }

    int opt = 1;
    setsockopt(m_listenfd.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(m_port);

    if (bind(m_listenfd.get(), (sockaddr*)&addr, sizeof(addr)) < 0) {
        LOGE("bind failed: %s", strerror(errno));
        m_listenfd.reset(-1);
        return;
    }
    if (listen(m_listenfd.get(), 64) < 0) {
        LOGE("listen failed: %s", strerror(errno));
        m_listenfd.reset(-1);
        return;
    }

    http_conn::setnonblocking(m_listenfd.get());
    // MainReactor 的 listen fd：ET + 无 ONESHOT
    http_conn::addfd(m_epollfd.get(), m_listenfd.get(), false);

    LOGI("MainReactor listening on 0.0.0.0:%d (listenfd=%d)",
         m_port, m_listenfd.get());
}

void Server::deal_new_conn() {
    sockaddr_in cli = {};
    socklen_t len = sizeof(cli);
    // ET 模式：循环 accept 直到 EAGAIN
    while (true) {
        int cfd = accept(m_listenfd.get(), (sockaddr*)&cli, &len);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            LOGE("MainReactor accept error: %s", strerror(errno));
            break;
        }
        if (cfd >= http_conn::MAX_FD) {
            ::close(cfd);
            continue;
        }
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cli.sin_addr, ip, sizeof(ip));
        LOGI("MainReactor accept %s:%d fd=%d",
             ip, ntohs(cli.sin_port), cfd);

        // Round-robin 选择一个 SubReactor（原子递增 + 取模）
        unsigned idx = m_rr_index++ % static_cast<unsigned>(m_num_subs);
        // 注意：多线程下 m_rr_index 可能被并发（但 MainReactor 是单线程 accept，
        // 这里只是单写，所以没问题；如未来多 acceptor 再改为 atomic 的 fetch_add）

        // 关键：设置非阻塞！SubReactor 那边 addfd 也会再设置一次，但为了保险
        // 这里先设置（SubReactor 的 setnonblocking 是幂等的）
        http_conn::setnonblocking(cfd);

        // 分发到目标 SubReactor。
        // SubReactor::dispatch_new_conn 内部加锁 + eventfd 唤醒目标 SubReactor，
        // 确保跨线程分发的安全（不会并发修改 SubReactor 的 epoll）
        m_subs[idx]->dispatch_new_conn(cfd, cli);
    }
}

void Server::start() {
    init_socket();
    // init_socket 失败（socket/bind/listen 任一失败）则不进入事件循环
    if (!m_listenfd) {
        LOGE("init_socket failed, server not starting");
        return;
    }

    // 启动所有 SubReactor 线程（先让它们跑起来，准备好接收分发的连接）
    for (auto& sub : m_subs) {
        sub->start();
    }

    LOGI("MainReactor event loop started");
    while (!m_stop.load()) {
        int n = epoll_wait(m_epollfd.get(), m_events.data(),
                           static_cast<int>(m_events.size()), -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOGE("MainReactor epoll_wait error: %s", strerror(errno));
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = m_events[i].data.fd;
            if (fd == m_listenfd.get()) {
                deal_new_conn();
            } else if (m_events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                LOGW("MainReactor unexpected event on fd=%d events=%u",
                     fd, (unsigned)m_events[i].events);
                // MainReactor 只处理 listenfd，理论上不会有其他 fd
            }
            // MainReactor 不处理 EPOLLIN/EPOLLOUT 业务事件（全在 SubReactor 里）
        }
    }
    LOGI("MainReactor event loop stopped");

    // 停止 SubReactors（join 它们的 I/O 线程）
    for (auto& sub : m_subs) {
        sub->stop();
    }
    // ThreadPool 将在 Server 析构时自动 shutdown join
}
