cmake_minimum_required(VERSION 3.10)
project(MasterSlaveReactorWebServer CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 调试信息 + 全部警告
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

# 头文件搜索路径为工程根目录
include_directories(${CMAKE_SOURCE_DIR})

add_executable(webserver
    main.cpp
    server.cpp
    sub_reactor.cpp
    thread_pool.cpp
    timer_heap.cpp
    http_conn.cpp
    
)

# 依赖: pthread (std::thread) + Linux 系统调用 (eventfd/epoll/socket/writev 等)
target_link_libraries(webserver pthread)
