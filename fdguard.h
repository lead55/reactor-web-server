#ifndef FDGUARD_H
#define FDGUARD_H

#include <unistd.h>

// RAII 文件描述符守卫：构造接管 fd，析构自动调用 close()。
//   - 典型 RAII 范式（构造获取资源 / 析构释放资源）
//   - 禁止拷贝语义（fd 是唯一所有权资源）
//   - 支持移动语义
class FdGuard {
public:
    explicit FdGuard(int fd = -1) noexcept : m_fd(fd) {}

    ~FdGuard() noexcept {
        if (m_fd != -1) {
            ::close(m_fd);
            m_fd = -1;
        }
    }

    // ---------- 禁止拷贝（Rule of 5）----------
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;

    // ---------- 允许移动（转移所有权）----------
    FdGuard(FdGuard&& other) noexcept : m_fd(other.m_fd) {
        other.m_fd = -1;
    }

    FdGuard& operator=(FdGuard&& other) noexcept {
        if (this != &other) {
            if (m_fd != -1) ::close(m_fd);
            m_fd = other.m_fd;
            other.m_fd = -1;
        }
        return *this;
    }

    // ---------- 工具接口 ----------
    // 隐式转换为 int，可直接传入系统调用
    operator int() const noexcept { return m_fd; }
    int  get()       const noexcept { return m_fd; }

    // 释放所有权（返回 fd 但不 close），用在要把 fd 交给别人管的场景
    int  release() noexcept {
        int fd = m_fd;
        m_fd = -1;
        return fd;
    }

    // 重置为新 fd；旧 fd 会被 close（防御 reset(g.get()) 自杀）
    void reset(int fd = -1) noexcept {
        if (fd != m_fd) {
            if (m_fd != -1) ::close(m_fd);
            m_fd = fd;
        }
    }

    // 判断是否持有有效 fd
    explicit operator bool() const noexcept { return m_fd != -1; }

private:
    int m_fd;
};

#endif // FDGUARD_H
