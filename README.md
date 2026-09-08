```
# 主从 Reactor 高性能 Web 服务器
基于 Linux epoll 实现的主从 Reactor + 线程池架构的 HTTP/1.1 静态文件 Web 服务器。

## 技术栈
C++14、Linux、epoll ET、主从 Reactor、线程池、sendfile

## 架构
- **MainReactor**（主线程）：epoll_wait 监听 listen fd，accept 新连接，round‑robin 分发
- **SubReactor**（多线程）：各自独立 epoll 处理 I/O 读写
- **ThreadPool**：处理 HTTP 解析、文件读取等业务逻辑

## 核心特性
- epoll ET（边缘触发）+ 非阻塞 socket
- eventfd 跨线程无锁唤醒，timerfd 驱动定时任务
- 小根堆定时器 + seq 版本号懒删除（O(1) 删除）
- sendfile() 零拷贝传输静态文件
- 双缓冲异步日志系统
- RAII FdGuard 自动管理 fd 生命周期

## 性能
wrk 压测：16 线程 / 10000 并发连接，QPS 22,288，平均延迟 44ms

## 编译运行
```bash
mkdir build && cd build
cmake ..
make
./webserver

##项目结构
├── main.cpp           # 程序入口
├── server.cpp/h       # MainReactor 主服务器
├── sub_reactor.cpp/h  # SubReactor 子反应堆
├── thread_pool.cpp/h  # 线程池
├── http_conn.cpp/h    # HTTP 连接处理
├── timer_heap.cpp/h   # 小根堆定时器
├── async_log.h        # 异步日志
├── fdguard.h          # RAII fd 管理
├── index.html         # 默认静态页面
└── CMakeLists.txt     # CMake 构建配置


```
