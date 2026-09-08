#ifndef HTTP_CONN_H
#define HTTP_CONN_H

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "async_log.h"

// 单个 HTTP 连接：使用有限状态机解析 HTTP/1.1 GET 请求，
// 支持 Keep-Alive 长连接，ET 模式下的非阻塞读写。
class http_conn {
public:

    static constexpr int READ_BUFFER_SIZE = 16384;
    static constexpr int MAX_FD = 65536;

    
    // 目前仅实现 GET 方法（静态文件服务器的核心需求）
    enum class Method     { GET };
    enum class CheckState { REQUEST_LINE, HEADER, BODY };
    enum class HttpCode {
        NoRequest,          // 请求不完整，需继续读
        GetRequest,         // 获得完整请求
        BadRequest,         // 请求语法错误
        NoResource,         // 资源不存在
        ForbiddenRequest,   // 资源禁止访问
        FileRequest,        // 文件请求正常
        InternalError       // 服务器内部错误
    };
    enum class LineStatus { Ok, Bad, Open };

    // 设置静态资源根目录
    static void set_root(const std::string& root) { m_root = root; }

    http_conn();
    ~http_conn();

    void init(int sockfd, const sockaddr_in& addr);
    void close_conn();

    // ET 模式下的读：循环读直到 EAGAIN
    bool read_once();
    // ET 模式下的写：循环写直到 EAGAIN；返回 0=错误 1=全部写完 2=需继续等待 EPOLLOUT
    int  write();

    // -------- 主从 Reactor + 线程池 拆分接口 --------
    // 1) I/O 层（在 SubReactor I/O 线程调用）：
    //    解析请求行/头/体；返回 0=请求不完整(继续 EPOLLIN) 1=请求/错误完整(可提交业务)
    int  prepare_io_read();
    // 2) 业务层（在线程池 worker 线程调用）：
    //    do_request()（GET/POST 文件读取）+ process_write() 组装响应
    //    返回 1=响应已就绪，SubReactor 应注册 EPOLLOUT
    //    返回 2=发生错误，SubReactor 应关闭连接
    int  run_business_and_prepare_write();

    bool keep_alive() const noexcept { return m_linger; }
    // 复位状态以处理连接上的下一个请求；返回 true 表示缓冲区中还有可立即处理的残留数据（pipeline）
    bool reset_for_next();

    int get_fd() const noexcept { return m_sockfd; }

    // epoll 辅助静态方法
    static int  setnonblocking(int fd);
    static void addfd(int epollfd, int fd, bool one_shot);
    static void modfd(int epollfd, int fd, int ev);
    static void removefd(int epollfd, int fd);

private:
    void init_state();

    // 从状态机：从读缓冲中解析出一行（以 \r\n 结尾）
    LineStatus parse_line();
    // 主状态机：驱动整个请求解析流程
    HttpCode process_read();
    // 主状态机各阶段的处理函数
    HttpCode parse_request_line(char* text);
    HttpCode parse_headers(char* text);
    HttpCode parse_body(char* text);
    // 处理请求：定位资源并读取
    HttpCode do_request();
    // 根据处理结果生成响应
    bool process_write(HttpCode ret);
    // 根据 URL 后缀返回 Content-Type
    const char* get_content_type(const char* url);
    // 取得当前待解析行的起始地址
    char* get_line() { return m_read_buf.data() + m_start_line; }

    int         m_sockfd;
    sockaddr_in m_address;

    std::vector<char> m_read_buf;  
    int  m_read_idx;      // 已读入读缓冲的字节数
    int  m_checked_idx;   // 当前正在解析的字节位置
    int  m_start_line;    // 当前解析行的起始位置

    // 解析状态
    CheckState  m_check_state;
    Method      m_method;
    
    std::string m_url;       
    std::string m_version;   
    std::string m_host;      
    bool        m_linger;          // 是否 Keep-Alive
    int         m_content_length;

    // 资源
    std::string m_file_content;   // 仅用于内联内容（如 "/" 欢迎页）
    // 零拷贝：静态文件不再读入内存，而是保存 fd + size，write() 时用 sendfile() 直传
    int         m_file_fd;        // 静态文件 fd（-1 = 无效）
    size_t      m_file_size;      // 静态文件大小
    bool        m_use_sendfile;   // true = write() 用 sendfile 零拷贝路径

    // 响应：头部 + 体（体仅用于错误响应等内联内容；文件请求走 sendfile）
    std::string m_response_head;
    std::string m_response_body;
    size_t      m_write_offset;   // 已发送字节数（含头部 + 体/文件）

    // ---------- 主从 Reactor：parse 结果传递给业务线程 ----------
    HttpCode m_parsed_code;

    static std::string m_root;   // 静态资源根目录
};

#endif // HTTP_CONN_H
