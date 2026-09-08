#include "server.h"
#include "async_log.h"
#include <csignal>
#include <climits>   // PATH_MAX
#include <string>

// 取可执行文件所在目录 (build/), 其上级目录作为静态资源根目录。
// 这样无论你在哪个工作目录启动 (build/、webServer/、/)，
// 都能正确找到 webServer/ 下的 big.bin、index.html 等文件。
static std::string resolve_root_from_exe(const char* argv0) {
    char buf[PATH_MAX];
    if (realpath(argv0, buf) == nullptr) return ".";

    std::string exe_path = buf;
    auto slash = exe_path.find_last_of('/');
    if (slash == std::string::npos) return ".";
    std::string exe_dir = exe_path.substr(0, slash);  // e.g. /mnt/webshare/build

    // 如果 exe_dir 以 /build 结尾, 取父目录 (符合标准构建布局)
    if (exe_dir.size() >= 6 && exe_dir.substr(exe_dir.size() - 6) == "/build") {
        return exe_dir.substr(0, exe_dir.size() - 6);
    }
    // 否则就用 exe_dir 本身
    return exe_dir;
}

int main(int argc, char* argv[]) {
    // 初始化异步日志系统（必须在任何 LOGE 之前，否则日志会丢失）
    //   参数3 (level)              : 文件里写哪些级别（Info = 写 INFO/WARN/ERROR, DEBUG 不写，省磁盘）
    //   参数4 (echo_stderr_level)  : 终端同步显示哪些级别
    //       开发模式: LogLevel::Debug  (默认, 全看到)
    //       生产模式: static_cast<LogLevel>(99)  (完全不打终端, 只写文件)
    AsyncLog::instance().init("./log", "server.log",
                              LogLevel::Info,        // 文件只记 INFO+
                              LogLevel::Debug);      // 终端全显示 

    int port = 9006;
    if (argc > 1) {
        
        try {
            port = std::stoi(argv[1]);
        } catch (...) {
            LOGE("invalid port: %s", argv[1]);
            return 1;
        }
    }
    if (port <= 0 || port > 65535) {
        LOGE("invalid port: %d", port);
        return 1;
    }

    // 忽略 SIGPIPE：向已关闭的 socket 写会触发，避免进程退出
    signal(SIGPIPE, SIG_IGN);

    std::string root = resolve_root_from_exe(argv[0]);
    LOGI("starting Epoll ET Reactor WebServer on port %d (Modern C++ build)", port);
    LOGI("static resource root: %s", root.c_str());

    Server server(port, root);
    server.start();

    return 0;
}
