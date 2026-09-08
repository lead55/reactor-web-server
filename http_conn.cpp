#include "http_conn.h"
#include <cerrno>
// 注意: sys/stat.h、fcntl.h、arpa/inet.h 等已通过 http_conn.h 包含，
// 此处不再重复 include


std::string http_conn::m_root = ".";

http_conn::http_conn() {
    init_state();
}

http_conn::~http_conn() {
    // RAII：析构自动关闭 socket（防止忘记 close_conn 导致 fd 泄漏）
    // 注意：close_conn 内部有 m_sockfd != -1 判断，多次调用安全
    close_conn();
}

void http_conn::init_state() {
    m_sockfd = -1;
    m_address = sockaddr_in{};
    m_read_idx = 0;
    m_checked_idx = 0;
    m_start_line = 0;

    m_check_state = CheckState::REQUEST_LINE;
    m_method = Method::GET;
   
    m_url.clear();
    m_version.clear();
    m_host.clear();
    m_linger = false;
    m_content_length = 0;
    m_file_content.clear();
    m_file_fd = -1;           // 零拷贝：无文件 fd
    m_file_size = 0;
    m_use_sendfile = false;
    m_response_head.clear();
    m_response_body.clear();
    m_write_offset = 0;
    m_parsed_code = HttpCode::NoRequest;
}

void http_conn::init(int sockfd, const sockaddr_in& addr) {
    init_state();          // 先复位所有解析/响应状态
    m_sockfd = sockfd;     // 再设置连接信息（注意：必须在 init_state 之后，否则会被重置为 -1）
    m_address = addr;
    // 按需分配读缓冲：首次使用该连接槽才分配 16KB；后续复用（close→init 循环）时已够尺寸，直接跳过
    if (m_read_buf.size() < READ_BUFFER_SIZE) {
        m_read_buf.resize(READ_BUFFER_SIZE);
    }
}

void http_conn::close_conn() {
    if (m_sockfd != -1) {
        LOGI("close connection fd=%d", m_sockfd);
        ::close(m_sockfd);
        m_sockfd = -1;
    }
    // 零拷贝：关闭可能残留的文件 fd
    if (m_file_fd != -1) {
        ::close(m_file_fd);
        m_file_fd = -1;
    }
    init_state();
}

// ---------------- epoll / fd 辅助 ----------------

int http_conn::setnonblocking(int fd) {
    int old_option = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, old_option | O_NONBLOCK);
    return old_option;
}

void http_conn::addfd(int epollfd, int fd, bool one_shot) {
    epoll_event event;
    event.data.fd = fd;
    // ET 边沿触发 + 对端关闭检测
    event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    if (one_shot) event.events |= EPOLLONESHOT;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
    setnonblocking(fd);   // 所有 socket 设为非阻塞
}

void http_conn::modfd(int epollfd, int fd, int ev) {
    epoll_event event;
    event.data.fd = fd;
    event.events = ev | EPOLLET | EPOLLRDHUP;
    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &event);
}

void http_conn::removefd(int epollfd, int fd) {
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, nullptr);
}

// ---------------- 读（ET） ----------------

bool http_conn::read_once() {
    if (m_read_idx >= static_cast<int>(m_read_buf.size())) {
        LOGW("read buffer full fd=%d", m_sockfd);
        return false;
    }
    // ET 模式：必须一次性把内核缓冲区读空
    int before = m_read_idx;   // 记录本次 read_once 前的游标，用于真实字节数统计
    while (true) {
        int bytes_read = recv(m_sockfd,
                              m_read_buf.data() + m_read_idx,   // C++: vector<char>::data()
                              m_read_buf.size() - m_read_idx,
                              0);
        if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;   // 内核暂无更多数据，读完了
            }
            if (errno == EINTR) continue;
            LOGE("recv error fd=%d: %s", m_sockfd, strerror(errno));
            return false;
        } else if (bytes_read == 0) {
            // 对端关闭写端（FIN）。
            // 如果本次已收到数据，不能丢弃——先 break 返回 true 让上层处理请求，
            // 处理完响应后 deal_write 的 keep_alive 路径会 modfd EPOLLIN，
            // 下一次 read_once 会再次收到 0 → false → close_connection。
            // （nc -q1 / curl --no-keepalive 等客户端发完请求就关写端，但读端还开着等响应）
            if (m_read_idx > before) {
                break;
            }
            return false;
        }
        m_read_idx += bytes_read;
    }
    int just_recv = m_read_idx - before;
    if (just_recv > 0) {
        LOGD("fd=%d recv %d bytes (total in buffer=%d)", m_sockfd, just_recv, m_read_idx);
    }
    return true;
}

// ---------------- 从状态机：解析一行 ----------------

http_conn::LineStatus http_conn::parse_line() {
    char temp;
    for (; m_checked_idx < m_read_idx; ++m_checked_idx) {
        temp = m_read_buf[m_checked_idx];
        if (temp == '\r') {
            if (m_checked_idx + 1 == m_read_idx) {
                return LineStatus::Open;   // \r 后还没有数据，需继续读
            }
            if (m_read_buf[m_checked_idx + 1] == '\n') {
                m_read_buf[m_checked_idx++] = '\0';
                m_read_buf[m_checked_idx++] = '\0';
                return LineStatus::Ok;
            }
            return LineStatus::Bad;
        } else if (temp == '\n') {
            // 容错：单独的 \n（某些客户端只发 \n）
            if (m_checked_idx >= 1 && m_read_buf[m_checked_idx - 1] == '\r') {
                m_read_buf[m_checked_idx - 1] = '\0';
                m_read_buf[m_checked_idx++] = '\0';
                return LineStatus::Ok;
            }
            return LineStatus::Bad;
        }
    }
    return LineStatus::Open;
}

// ---------------- 主状态机 ----------------

http_conn::HttpCode http_conn::process_read() {
    LineStatus line_status = LineStatus::Ok;
    HttpCode ret = HttpCode::NoRequest;

    // CheckState::BODY 且已有完整行 -> 直接处理；否则先解析一行
    while ((m_check_state == CheckState::BODY && line_status == LineStatus::Ok) ||
           ((line_status = parse_line()) == LineStatus::Ok)) {
        char* text = get_line();
        m_start_line = m_checked_idx;   // 指向下一行起始位置
        LOGD("parse [%s]", text);

        switch (m_check_state) {
        case CheckState::REQUEST_LINE: {
            ret = parse_request_line(text);
            if (ret == HttpCode::BadRequest) return HttpCode::BadRequest;
            break;
        }
        case CheckState::HEADER: {
            ret = parse_headers(text);
            if (ret == HttpCode::BadRequest) return HttpCode::BadRequest;
            else if (ret == HttpCode::GetRequest) return do_request();
            break;
        }
        case CheckState::BODY: {
            ret = parse_body(text);
            if (ret == HttpCode::GetRequest) return do_request();
            line_status = LineStatus::Open;
            break;
        }
        default:
            return HttpCode::InternalError;
        }
    }
    return HttpCode::NoRequest;   // 请求不完整，需继续读
}

http_conn::HttpCode http_conn::parse_request_line(char* text) {
    // 格式严格: METHOD SP URL SP VERSION  —— 只能单空格分隔，多空格/制表符直接 400
    // 理由：项目硬约束 + RFC 7230 §3.1.1 允许实现者拒绝多余空白以简化解析
    char* p = strchr(text, ' ');
    if (!p) return HttpCode::BadRequest;
    *p++ = '\0';

    // 强制单空格：METHOD 与 URL 之间只能有一个 SP
    //   strchr 找到的是第一个空格。如果第二个字符也是空格/制表，说明多空格，直接拒
    if (*p == ' ' || *p == '\t') return HttpCode::BadRequest;

    // 目前仅支持 GET（静态文件服务器）；非 GET 统一 400 Bad Request
    if (strcasecmp(text, "GET") == 0)            m_method = Method::GET;
    else                                         return HttpCode::BadRequest;

    char* url = p;
    p = strchr(p, ' ');
    if (!p) return HttpCode::BadRequest;
    *p++ = '\0';
    // 强制单空格：URL 与 VERSION 之间也只能有一个 SP
    if (*p == ' ' || *p == '\t') return HttpCode::BadRequest;
    // C++ 风格：std::string 赋值，自动管理长度，无 strncpy 截断风险
    m_url = url;

    // 版本（URL 后面第一个 SP 之后必须紧跟 "HTTP/x.y"，不允许前置空白）
    if (strncasecmp(p, "HTTP/1.1", 8) == 0) {
        m_version = "HTTP/1.1";
        m_linger = true;   // HTTP/1.1 默认 Keep-Alive
    } else if (strncasecmp(p, "HTTP/1.0", 8) == 0) {
        m_version = "HTTP/1.0";
        m_linger = false;  // HTTP/1.0 默认关闭
    } else {
        return HttpCode::BadRequest;
    }

    m_check_state = CheckState::HEADER;
    return HttpCode::NoRequest;
}

http_conn::HttpCode http_conn::parse_headers(char* text) {
    // 空行：头部结束
    if (text[0] == '\0') {
        // HTTP/1.1 (RFC 7230 §5.4) 必须包含 Host: 头, 否则 400 Bad Request
        if (m_version == "HTTP/1.1" && m_host.empty()) {
            LOGW("HTTP/1.1 request missing Host header");
            return HttpCode::BadRequest;
        }
        if (m_content_length != 0) {
            m_check_state = CheckState::BODY;
            return HttpCode::NoRequest;
        }
        return HttpCode::GetRequest;   // GET 请求到这里就完整了
    }
    if (strncasecmp(text, "Host:", 5) == 0) {
        text += 5;
        text += strspn(text, " \t");
        m_host = text;   // C++: std::string 赋值，自动截断问题消除
    } else if (strncasecmp(text, "Connection:", 11) == 0) {
        text += 11;
        text += strspn(text, " \t");
        m_linger = (strcasecmp(text, "keep-alive") == 0);
    } else if (strncasecmp(text, "Content-Length:", 15) == 0) {
        text += 15;
        text += strspn(text, " \t");
        // 严格校验 + 防整数溢出（代替 atoi 的 UB 风险）
        //   1) 纯数字（可选 + 或 -），长度不超过 10 位（int32 最大值 2^31-1 = 2147483647 共 10 位）
        //   2) strtol 解析，errno == ERANGE 表示 libc 层面溢出
        //   3) 值必须在 [0, READ_BUFFER_SIZE) 范围内
        {
            const char* p = text;
            if (*p == '-' || *p == '+') ++p;
            int digits = 0;
            while ('0' <= p[digits] && p[digits] <= '9') ++digits;
            if (digits == 0 || digits > 10 || p[digits] != '\0') {
                LOGW("bad Content-Length format: %s", text);
                return HttpCode::BadRequest;
            }
        }
        errno = 0;
        char* endp = nullptr;
        long val = strtol(text, &endp, 10);
        if (errno == ERANGE || val < 0L || val >= static_cast<long>(READ_BUFFER_SIZE)) {
            LOGW("invalid Content-Length: %s (out of range)", text);
            return HttpCode::BadRequest;
        }
        m_content_length = static_cast<int>(val);
    } else {
        // 未知头部：HTTP 头必须是 "Name: Value" 格式，含冒号才算合法
        if (strchr(text, ':') == nullptr) {
            LOGW("bad header line: %s", text);
            return HttpCode::BadRequest;
        }
        // 其余合法但未处理的头部忽略
    }
    return HttpCode::NoRequest;
}

http_conn::HttpCode http_conn::parse_body(char* text) {
    // 安全校验：防止 Content-Length 为负或溢出导致越界写
    if (m_content_length < 0 || m_content_length >= READ_BUFFER_SIZE) {
        LOGW("invalid Content-Length: %d", m_content_length);
        return HttpCode::BadRequest;
    }
    if (m_read_idx >= m_content_length + m_checked_idx) {
        text[m_content_length] = '\0';
        return HttpCode::GetRequest;
    }
    return HttpCode::NoRequest;
}

http_conn::HttpCode http_conn::do_request() {
    if (m_url == "/") {
        // "/" 返回内置欢迎页（内联内容，不走 sendfile）
        m_file_content =
            "<!DOCTYPE html>"
            "<html><head><meta charset=\"utf-8\">"
            "<title>Reactor WebServer (Modern C++)</title></head>"
            "<body>"
            "<h1>Hello from Epoll ET Reactor WebServer!</h1>"
            "<p>HTTP/1.1 Keep-Alive enabled.</p>"
            "<p>Try: <a href=\"/index.html\">/index.html</a></p>"
            "<p style=\"color:#666;font-size:0.9em\">"
            "Written in <b>modern C++11 style</b>: RAII, enum class, "
            "std::vector, std::string, std::unique_ptr."
            "</p></body></html>";
        m_use_sendfile = false;   // 内联内容用 writev
        return HttpCode::FileRequest;
    }

    // 其它路径：尝试从根目录读取静态文件
    // 安全：防止目录穿越攻击（如 /../../etc/passwd）
    if (m_url.find("..") != std::string::npos) {
        LOGW("path traversal blocked: %s", m_url.c_str());
        return HttpCode::ForbiddenRequest;
    }
    std::string path = m_root + m_url;   // C++: std::string 拼接，代替 snprintf

    // 局部 struct stat: 仅本函数使用, 不作为成员节省 ~144B × MAX_FD 内存
    struct stat file_stat = {};
    if (stat(path.c_str(), &file_stat) < 0) {
        LOGW("resource not found: %s", path.c_str());
        return HttpCode::NoResource;
    }
    if (S_ISDIR(file_stat.st_mode)) return HttpCode::NoResource;
    if (!S_ISREG(file_stat.st_mode)) return HttpCode::ForbiddenRequest;

    // 零拷贝：打开文件 fd，保存 fd + size，不读入内存
    // sendfile() 会直接从内核页缓存传输到 socket，绕过用户态缓冲区
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        LOGW("open failed: %s (%s)", path.c_str(), strerror(errno));
        return HttpCode::NoResource;
    }
    m_file_fd   = fd;
    m_file_size = static_cast<size_t>(file_stat.st_size);
    m_use_sendfile = true;
    return HttpCode::FileRequest;
}

// ---------------- 响应生成 ----------------

const char* http_conn::get_content_type(const char* url) {
    const char* dot = strrchr(url, '.');
    if (!dot) return "text/plain";
    // 大小写不敏感比较
    if (strcasecmp(dot, ".html") == 0 || strcasecmp(dot, ".htm") == 0) return "text/html";
    if (strcasecmp(dot, ".css") == 0)  return "text/css";
    if (strcasecmp(dot, ".js") == 0)   return "application/javascript";
    if (strcasecmp(dot, ".png") == 0)  return "image/png";
    if (strcasecmp(dot, ".jpg") == 0 || strcasecmp(dot, ".jpeg") == 0) return "image/jpeg";
    if (strcasecmp(dot, ".gif") == 0)  return "image/gif";
    if (strcasecmp(dot, ".txt") == 0)  return "text/plain";
    return "application/octet-stream";
}

bool http_conn::process_write(HttpCode ret) {
    m_response_head.clear();
    m_response_body.clear();
    m_write_offset = 0;

    int status = 200;
    const char* title = "OK";
    bool serve_file = false;

    switch (ret) {
    case HttpCode::FileRequest:
        status = 200; title = "OK"; serve_file = true;
        break;
    case HttpCode::BadRequest:
        status = 400; title = "Bad Request"; m_linger = false;
        m_response_body = "<html><body><h1>400 Bad Request</h1></body></html>";
        break;
    case HttpCode::ForbiddenRequest:
        status = 403; title = "Forbidden"; m_linger = false;
        m_response_body = "<html><body><h1>403 Forbidden</h1></body></html>";
        break;
    case HttpCode::NoResource:
        status = 404; title = "Not Found";
        m_response_body = "<html><body><h1>404 Not Found</h1></body></html>";
        break;
    case HttpCode::InternalError:
        status = 500; title = "Internal Server Error"; m_linger = false;
        m_response_body = "<html><body><h1>500 Internal Server Error</h1></body></html>";
        break;
    default:
        return false;
    }

    const char* ctype = "text/html";
    if (serve_file && !m_url.empty() && m_url != "/") {
        ctype = get_content_type(m_url.c_str());
    }
    // 零拷贝：sendfile 路径用 m_file_size，否则用内存中的 body 大小
    size_t body_len;
    if (serve_file) {
        body_len = m_use_sendfile ? m_file_size : m_file_content.size();
    } else {
        body_len = m_response_body.size();
    }

    // HTTP 头拼装：保留 snprintf（性能考虑，热点路径上用格式化字符串高效拼接"）
    char head[512];
    int n = snprintf(head, sizeof(head),
        "HTTP/1.1 %d %s\r\n"
        "Server: EpollETReactor/1.0 (Modern C++)\r\n"
        "Content-Length: %zu\r\n"
        "Content-Type: %s\r\n"
        "Connection: %s\r\n"
        "\r\n",
        status, title, body_len, ctype, m_linger ? "keep-alive" : "close");
    m_response_head.assign(head, n);

    // 零拷贝：sendfile 路径不拷贝文件内容到 body；内联内容才走 writev
    if (serve_file && !m_use_sendfile) m_response_body = std::move(m_file_content);

    LOGI("respond fd=%d %d %s len=%zu keep-alive=%d",
         m_sockfd, status, title, body_len, m_linger);
    return true;
}

// ---------------- 写（ET） ----------------

int http_conn::write() {
    // ===== 零拷贝路径：sendfile 直接从内核页缓存传到 socket =====
    if (m_use_sendfile) {
        // Phase 1: 发送 HTTP 响应头（小数据，用 send）
        while (m_write_offset < m_response_head.size()) {
            ssize_t s = ::send(m_sockfd,
                m_response_head.data() + m_write_offset,
                m_response_head.size() - m_write_offset,
                MSG_NOSIGNAL);
            if (s <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return 2;
                if (errno == EINTR) continue;
                LOGE("send head error fd=%d: %s", m_sockfd, strerror(errno));
                return 0;
            }
            m_write_offset += s;
        }
        // Phase 2: sendfile 传输文件体（零用户态拷贝）
        off_t file_offset = static_cast<off_t>(m_write_offset - m_response_head.size());
        if (file_offset < 0 || static_cast<size_t>(file_offset) >= m_file_size) return 1;
        size_t remaining = m_file_size - static_cast<size_t>(file_offset);
        while (remaining > 0) {
            // sendfile 内部会更新 file_offset
            ssize_t s = ::sendfile(m_sockfd, m_file_fd, &file_offset, remaining);
            if (s <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return 2;
                if (errno == EINTR) continue;
                LOGE("sendfile error fd=%d: %s", m_sockfd, strerror(errno));
                return 0;
            }
            m_write_offset += s;
            remaining -= static_cast<size_t>(s);
        }
        return 1;   // 全部发送完成
    }

    // ===== 普通路径：writev 聚集写头部 + 内存体（错误响应 / 内联欢迎页）=====
    size_t total = m_response_head.size() + m_response_body.size();
    while (m_write_offset < total) {
        struct iovec iv[2];
        int n = 0;

        size_t head_sent = (m_write_offset < m_response_head.size())
                           ? m_write_offset : m_response_head.size();
        if (head_sent < m_response_head.size()) {
            iv[n].iov_base = const_cast<char*>(m_response_head.data()) + head_sent;
            iv[n].iov_len  = m_response_head.size() - head_sent;
            ++n;
        }
        if (!m_response_body.empty() && m_write_offset >= m_response_head.size()) {
            size_t body_sent = m_write_offset - m_response_head.size();
            iv[n].iov_base = const_cast<char*>(m_response_body.data()) + body_sent;
            iv[n].iov_len  = m_response_body.size() - body_sent;
            ++n;
        }

        ssize_t s = writev(m_sockfd, iv, n);
        if (s <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 2;
            }
            if (errno == EINTR) continue;
            LOGE("writev error fd=%d: %s", m_sockfd, strerror(errno));
            return 0;
        }
        m_write_offset += s;
    }
    return 1;
}

// ---------------- 复位 ----------------

bool http_conn::reset_for_next() {
    // 残留数据 = 已解析请求之后到读缓冲末尾
    // 防御: 状态机保证 m_start_line <= m_read_idx, 但异常请求下可能偏离,
    // 这里用无符号减法 + 边界判断避免 int -> size_t 负数下溢导致 memmove 越界
    if (m_start_line < 0 || m_start_line > m_read_idx) {
        m_read_idx = 0;
    } else {
        int leftover = m_read_idx - m_start_line;
        if (leftover > 0) {
            // vector<char> 的 data() 返回连续内存，memmove 仍然成立
            memmove(m_read_buf.data(),
                    m_read_buf.data() + m_start_line,
                    static_cast<size_t>(leftover));
            m_read_idx = leftover;
        } else {
            m_read_idx = 0;
        }
    }

    m_checked_idx = 0;
    m_start_line = 0;
    m_check_state = CheckState::REQUEST_LINE;
    m_method = Method::GET;
    m_url.clear();       
    m_version.clear();
    m_host.clear();
    m_linger = false;     // 由下一个请求行/头部重新决定
    m_content_length = 0;
    m_file_content.clear();
    // 零拷贝：关闭上一个响应可能残留的文件 fd
    if (m_file_fd != -1) {
        ::close(m_file_fd);
        m_file_fd = -1;
    }
    m_file_size = 0;
    m_use_sendfile = false;
    m_response_head.clear();
    m_response_body.clear();
    m_write_offset = 0;
    m_parsed_code = HttpCode::NoRequest;

    return m_read_idx > 0;   // 是否有可立即处理的 pipeline 数据
}

// ---------------- 主从 Reactor 拆分接口 ----------------

int http_conn::prepare_io_read() {
    HttpCode ret = process_read();
    if (ret == HttpCode::NoRequest) {
        return 0;   // 请求不完整，继续等待 EPOLLIN
    }
    // 记录 parse 结果（包括 BadRequest 等错误码），由业务线程继续处理
    m_parsed_code = ret;
    return 1;       // parse 完成（无论成功/失败），可提交业务
}

int http_conn::run_business_and_prepare_write() {
    HttpCode code = m_parsed_code;
    // 如果 parse 阶段得到的是 GetRequest（意味着头部/体解析完成，但还没定位资源），
    // 则在这里执行 do_request() 去读文件/判断权限——这是可能阻塞的业务逻辑
    if (code == HttpCode::GetRequest) {
        code = do_request();
    }
    if (!process_write(code)) {
        return 2;   // 响应生成失败：关闭连接
    }
    return 1;       // 响应已写入 m_response_head / m_response_body：注册 EPOLLOUT
}
